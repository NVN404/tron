use common::{AppError, Result};
use sqlx::PgPool;
use tracing::info;

pub struct SecuritySelfCheck;

impl SecuritySelfCheck {
    pub async fn run(pool: &PgPool) -> Result<()> {
        info!("Running security startup self-checks on analytics database connection...");

        // 1. Verify current role is NOT a superuser and does NOT have BYPASSRLS
        let role_row: Option<(bool, bool)> = sqlx::query_as(
            "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user"
        )
        .fetch_optional(pool)
        .await?;

        if let Some((is_super, bypass_rls)) = role_row {
            if is_super {
                return Err(AppError::TenantViolation(
                    "FATAL SECURITY VIOLATION: API database connection is running as SUPERUSER! Refusing to boot.".to_string()
                ));
            }
            if bypass_rls {
                return Err(AppError::TenantViolation(
                    "FATAL SECURITY VIOLATION: API database role has BYPASSRLS privilege! Refusing to boot.".to_string()
                ));
            }
        }

        // 2. Verify current role does NOT own protected analytics tables
        let owned_tables: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tableowner = current_user AND tablename IN ('agg_hourly', 'agg_hourly_product', 'bucket_state')"
        )
        .fetch_one(pool)
        .await?;

        if owned_tables.0 > 0 {
            return Err(AppError::TenantViolation(
                "FATAL SECURITY VIOLATION: API database role owns protected rollup tables! Table owners can bypass RLS. Refusing to boot.".to_string()
            ));
        }

        // 3. Verify FORCE ROW LEVEL SECURITY on all protected rollup tables
        let tables = ["agg_hourly", "agg_hourly_product", "bucket_state"];
        for table in tables {
            let row: Option<(bool, bool)> = sqlx::query_as(
                "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = $1"
            )
            .bind(table)
            .fetch_optional(pool)
            .await?;

            match row {
                Some((rls_enabled, rls_forced)) => {
                    if !rls_enabled {
                        return Err(AppError::TenantViolation(format!(
                            "FATAL SECURITY VIOLATION: Table '{}' lacks ROW LEVEL SECURITY!", table
                        )));
                    }
                    if !rls_forced {
                        return Err(AppError::TenantViolation(format!(
                            "FATAL SECURITY VIOLATION: Table '{}' lacks FORCE ROW LEVEL SECURITY!", table
                        )));
                    }
                }
                None => {
                    return Err(AppError::TenantViolation(format!(
                        "Protected table '{}' does not exist in analytics database!", table
                    )));
                }
            }
        }

        info!("🛡️ Security self-check passed: api_ro role is non-superuser, NOBYPASSRLS, not table owner, and FORCE RLS is verified on all rollup tables.");
        Ok(())
    }
}

