use anyhow::Result;
use chrono::{Duration, Utc};
use rand::{rngs::StdRng, Rng, SeedableRng};
use sqlx::PgPool;
use tracing::info;
use uuid::Uuid;

pub async fn run_update_mutations(pool: &PgPool, count: usize, seed: u64) -> Result<()> {
    let mut rng = StdRng::seed_from_u64(seed);
    info!("Running {} random update mutations (seed={})...", count, seed);

    // Fetch random orders to mutate
    let order_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM orders TABLESAMPLE SYSTEM (5) LIMIT $1"
    )
    .bind(count as i64)
    .fetch_all(pool)
    .await?;

    if order_ids.is_empty() {
        info!("No orders found to mutate. Please seed the database first.");
        return Ok(());
    }

    let mut amount_changes = 0;
    let mut status_changes = 0;
    let mut time_shifts = 0;
    let mut soft_deletes = 0;

    for &id in &order_ids {
        let action = rng.gen_range(0..4);
        match action {
            0 => {
                // Mutate amount
                let new_amount: i64 = rng.gen_range(500..500_000);
                sqlx::query("UPDATE orders SET amount_paise = $1, updated_at = NOW() WHERE id = $2")
                    .bind(new_amount)
                    .bind(id)
                    .execute(pool)
                    .await?;
                amount_changes += 1;
            }
            1 => {
                // Mutate status
                let statuses = ["completed", "pending", "cancelled", "refunded"];
                let new_status = statuses[rng.gen_range(0..statuses.len())];
                sqlx::query("UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2")
                    .bind(new_status)
                    .bind(id)
                    .execute(pool)
                    .await?;
                status_changes += 1;
            }
            2 => {
                // Shift timestamp across hour boundary
                let shift_minutes: i64 = rng.gen_range(-180..180);
                sqlx::query("UPDATE orders SET created_at = created_at + ($1 || ' minutes')::interval, updated_at = NOW() WHERE id = $2")
                    .bind(shift_minutes.to_string())
                    .bind(id)
                    .execute(pool)
                    .await?;
                time_shifts += 1;
            }
            3 => {
                // Soft delete
                sqlx::query("UPDATE orders SET deleted_at = NOW(), updated_at = NOW() WHERE id = $1")
                    .bind(id)
                    .execute(pool)
                    .await?;
                soft_deletes += 1;
            }
            _ => {}
        }
    }

    // Also insert a few backdated orders
    let backdated_count = (count / 10).max(5);
    let sample_tenants: Vec<(Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT tenant_id, product_id, category FROM products LIMIT 10"
    )
    .fetch_all(pool)
    .await?;

    if !sample_tenants.is_empty() {
        for _ in 0..backdated_count {
            let choice = &sample_tenants[rng.gen_range(0..sample_tenants.len())];
            let days_ago = rng.gen_range(1..60);
            let backdated_time = Utc::now() - Duration::days(days_ago);
            let amount: i64 = rng.gen_range(1000..100_000);
            sqlx::query(
                "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
                 VALUES ($1, $2, $3, 'completed', $4, $5)"
            )
            .bind(choice.0)
            .bind(choice.1)
            .bind(&choice.2)
            .bind(amount)
            .bind(backdated_time)
            .execute(pool)
            .await?;
        }
    }

    info!(
        "✅ Mutation run complete: {} amount changes, {} status changes, {} time shifts, {} soft deletes, {} backdated inserts",
        amount_changes, status_changes, time_shifts, soft_deletes, backdated_count
    );

    Ok(())
}
