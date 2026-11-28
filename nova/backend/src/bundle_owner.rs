//! Which group a bundle name belongs to (stardelt/stardelt#2230, option b). The first group that
//! records a deploy of a bundle or imports a dashboard for it claims the name; every later call
//! for that bundle with another group is refused, administrators included.

/// The sentence for a call whose group is not the bundle's.
pub fn other_group_sentence(bundle: &str, owner: &str) -> String {
    format!(
        "bundle `{bundle}` belongs to group `{owner}`; only that group may deploy it or change its dashboards"
    )
}

/// Why `asked` may not act for `bundle` when `stored` is its recorded owner, or `None`.
pub fn refusal(bundle: &str, stored: Option<&str>, asked: &str) -> Option<String> {
    match stored {
        Some(owner) if owner != asked.trim() => Some(other_group_sentence(bundle, owner)),
        _ => None,
    }
}

/// The recorded owner group of `bundle`, or `None` when no group has claimed it.
pub async fn owner_of(pool: &sqlx::PgPool, bundle: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT owner_group FROM bundle_owner WHERE bundle = $1")
        .bind(bundle.trim())
        .fetch_optional(pool)
        .await
}

/// Claim `bundle` for `owner_group` when no group holds it; answers the owner afterwards
/// (the caller's group on a fresh claim, the earlier group otherwise).
pub async fn claim(
    pool: &sqlx::PgPool,
    bundle: &str,
    owner_group: &str,
    actor: &str,
) -> Result<String, sqlx::Error> {
    sqlx::query(
        "INSERT INTO bundle_owner (bundle, owner_group, claimed_by) VALUES ($1, $2, $3) \
         ON CONFLICT (bundle) DO NOTHING",
    )
    .bind(bundle.trim())
    .bind(owner_group.trim())
    .bind(actor)
    .execute(pool)
    .await?;
    sqlx::query_scalar("SELECT owner_group FROM bundle_owner WHERE bundle = $1")
        .bind(bundle.trim())
        .fetch_one(pool)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusal_passes_an_unclaimed_bundle_and_the_owner() {
        assert_eq!(refusal("shop", None, "visitors"), None);
        assert_eq!(refusal("shop", Some("analytics"), "analytics"), None);
    }

    #[test]
    fn refusal_names_the_owner_to_another_group() {
        let sentence = refusal("shop", Some("analytics"), "visitors").expect("refused");
        assert!(
            sentence.contains("belongs to group `analytics`"),
            "{sentence}"
        );
    }

    /// One deployment row straight into the store (the shape `deployments::tests::insert`
    /// uses), so the backfill has a bundle with two groups.
    async fn insert(pool: &sqlx::PgPool, owner: &str, bundle: &str, target: &str, seconds: f64) {
        sqlx::query(
            "INSERT INTO deployment \
             (bundle, owner_group, target, status, pipelines, actor, actor_name, created_at) \
             VALUES ($1, $2, $3, 'applied', '[]'::jsonb, 'test', 'test', now() + make_interval(secs => $4))",
        )
        .bind(bundle)
        .bind(owner)
        .bind(target)
        .bind(seconds)
        .execute(pool)
        .await
        .expect("insert a deployment");
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn the_first_claim_wins_and_a_second_group_reads_it() {
        let pool = crate::db::test_support::migrated_pool().await;
        assert_eq!(
            claim(&pool, "shop", "analytics", "a").await.expect("claim"),
            "analytics"
        );
        assert_eq!(
            claim(&pool, "shop", "visitors", "b").await.expect("claim"),
            "analytics"
        );
        assert_eq!(
            owner_of(&pool, "shop").await.expect("read"),
            Some("analytics".to_string())
        );
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn the_backfill_claims_each_bundle_for_its_oldest_row() {
        let pool = crate::db::test_support::migrated_pool().await;
        insert(&pool, "analytics", "shop", "prod", -60.0).await;
        insert(&pool, "visitors", "shop", "prod", 0.0).await;
        // A row stored with surrounding whitespace (`valid_name` accepts it) is the same bundle.
        insert(&pool, "finance", " sales ", "prod", -120.0).await;
        insert(&pool, "visitors", "sales", "prod", -30.0).await;
        insert(&pool, "analytics", "\tmetrics", "prod", -200.0).await;
        // A name that trims to nothing cannot be claimed; it must not stop the migration.
        insert(&pool, "visitors", "  ", "prod", -300.0).await;
        sqlx::query("DELETE FROM bundle_owner")
            .execute(&pool)
            .await
            .expect("clear the claims");
        sqlx::query(
            "INSERT INTO bundle_owner (bundle, owner_group, claimed_by, claimed_at) \
             SELECT DISTINCT ON (trimmed_bundle) trimmed_bundle, trimmed_group, actor, created_at \
             FROM ( \
                 SELECT BTRIM(bundle, E' \\t\\n\\r\\f\\v') AS trimmed_bundle, \
                        BTRIM(owner_group, E' \\t\\n\\r\\f\\v') AS trimmed_group, \
                        actor, created_at, id \
                 FROM deployment \
             ) AS names \
             WHERE trimmed_bundle <> '' AND trimmed_group <> '' \
             ORDER BY trimmed_bundle, created_at ASC, id ASC",
        )
        .execute(&pool)
        .await
        .expect("backfill the claims");
        assert_eq!(
            owner_of(&pool, "shop").await.expect("read"),
            Some("analytics".to_string())
        );
        assert_eq!(
            owner_of(&pool, "sales").await.expect("read"),
            Some("finance".to_string()),
            "the trimmed name of the oldest row"
        );
        assert_eq!(
            owner_of(&pool, "metrics").await.expect("read"),
            Some("analytics".to_string()),
            "a name stored with a leading tab"
        );
        assert_eq!(owner_of(&pool, "").await.expect("read"), None);
    }
}
