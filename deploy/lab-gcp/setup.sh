#!/usr/bin/env bash
# One-time setup: lets GitHub Actions in ONE repository act in ONE Google
# Cloud project, with no key file anywhere. Run it yourself, in Cloud Shell,
# logged in as the project's owner:
#
#   ./setup.sh PROJECT_ID
#
# It creates a service account limited to Compute Engine and SSH through
# Identity-Aware Proxy, and a workload identity provider that only accepts
# tokens from the repository named below. Undo everything with:
#
#   ./setup.sh PROJECT_ID --remove
#
# The gcloud commands follow the google-github-actions/auth README
# ("Workload Identity Federation through a Service Account").
set -euo pipefail

PROJECT_ID="${1:?usage: setup.sh PROJECT_ID [--remove]}"
REPO="${DUET_REPO:-shpwrck/duet}"
SA_NAME="duet-lab"
SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
POOL="github"
PROVIDER="duet-repo"
ROLES=(roles/compute.admin roles/compute.osAdminLogin roles/iap.tunnelResourceAccessor)

if [[ "${2:-}" == "--remove" ]]; then
  for role in "${ROLES[@]}"; do
    gcloud projects remove-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SA}" --role="$role" --quiet >/dev/null || true
  done
  gcloud iam workload-identity-pools providers delete "$PROVIDER" --project="$PROJECT_ID" --location=global --workload-identity-pool="$POOL" --quiet || true
  gcloud iam service-accounts delete "$SA" --project="$PROJECT_ID" --quiet || true
  echo "Removed. The workload identity pool '$POOL' is left in place; delete it in the console if nothing else uses it."
  exit 0
fi

echo "Enabling APIs..."
gcloud services enable compute.googleapis.com iam.googleapis.com iamcredentials.googleapis.com \
  iap.googleapis.com sts.googleapis.com --project="$PROJECT_ID"

echo "Creating service account ${SA}..."
gcloud iam service-accounts describe "$SA" --project="$PROJECT_ID" >/dev/null 2>&1 ||
  gcloud iam service-accounts create "$SA_NAME" --project="$PROJECT_ID" --display-name="duet lab runner"

for role in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SA}" --role="$role" --condition=None --quiet >/dev/null
done

echo "Creating workload identity pool and provider..."
gcloud iam workload-identity-pools describe "$POOL" --project="$PROJECT_ID" --location=global >/dev/null 2>&1 ||
  gcloud iam workload-identity-pools create "$POOL" --project="$PROJECT_ID" --location=global --display-name="GitHub Actions Pool"

POOL_ID="$(gcloud iam workload-identity-pools describe "$POOL" --project="$PROJECT_ID" --location=global --format='value(name)')"

# Only tokens issued to workflows in this one repository are admitted.
gcloud iam workload-identity-pools providers describe "$PROVIDER" --project="$PROJECT_ID" --location=global --workload-identity-pool="$POOL" >/dev/null 2>&1 ||
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
    --project="$PROJECT_ID" --location=global --workload-identity-pool="$POOL" \
    --display-name="duet repository" \
    --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
    --attribute-condition="assertion.repository == '${REPO}'" \
    --issuer-uri="https://token.actions.githubusercontent.com"

gcloud iam service-accounts add-iam-policy-binding "$SA" --project="$PROJECT_ID" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${POOL_ID}/attribute.repository/${REPO}" --quiet >/dev/null

PROVIDER_ID="$(gcloud iam workload-identity-pools providers describe "$PROVIDER" --project="$PROJECT_ID" --location=global --workload-identity-pool="$POOL" --format='value(name)')"

cat <<DONE

Done. None of the three values below is a secret; they are identifiers.
Set them as repository VARIABLES (Settings > Secrets and variables > Actions
> Variables) in ${REPO}, or paste them to whoever is setting up the repo:

  GCP_PROJECT=${PROJECT_ID}
  GCP_SERVICE_ACCOUNT=${SA}
  GCP_WIF_PROVIDER=${PROVIDER_ID}

It can take up to five minutes for the new permissions to take effect.
DONE
