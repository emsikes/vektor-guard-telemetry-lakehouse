# ----------------------------------------------------------------------------
# Databricks provider and Unity Catalog resources.
#
# Catalog is managed in the Databricks UI (Free Edition limitation) - we
# reference it as a data source so Terraform knows it exists without
# trying to create or manage it.
#
# Schema and volume are managed by Terraform. They live inside the
# catalog and can be torn down and rebuilt programmatically.
#
# Auth: workspace URL and PAT are pulled from AWS Secrets Manager and
# SSM Parameter Store at plan time. This means the PAT lands in
# Terraform state - acceptable for Phase 1 (local gitignored state),
# resolved in Phase 2 via service principal with OAuth.
# ----------------------------------------------------------------------------

# Look up the Databricks workspace URL and PAT from our existing AWS storage.
data "aws_ssm_parameter" "databricks_workspace_url_lookup" {
    name = aws_ssm_parameter.databricks_workspace_url.name
}

data "aws_secretsmanager_secret_version" "databricks_pat_lookup" {
    secret_id = aws_secretsmanager_secret.databricks_pat.id
}

# Configure the Databricks provider with values from AWS managed secrets
provider "databricks" {
    host  = data.aws_ssm_parameter.databricks_workspace_url_lookup.value
    token = data.aws_secretsmanager_secret_version.databricks_pat_lookup.secret_string
}

# Reference the existing catalog as a data source (created via UI on Free Edition)
data "databricks_catalog" "vektor_guard_dp" {
    name = "vektor_guard_dp"
}

# Manage the bronze schema via Terraform
resource "databricks_schema" "bronze" {
    catalog_name = data.databricks_catalog.vektor_guard_dp.name
    name         = "bronze"
    comment      = "Raw ingested events from runtime FastAPI"
}

# Manage the landing zone volume via Terraform 
resource "databricks_volume" "landing" {
    catalog_name    = data.databricks_catalog.vektor_guard_dp.name
    schema_name     = databricks_schema.bronze.name
    name            = "landing"
    volume_type     = "MANAGED"
    comment         = "JSON files landing zone for bronze ingestion"  
}

