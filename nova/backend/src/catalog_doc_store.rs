//! The catalog documentation index (design ticket stardelt-nova#104, search slice 1).

use sqlx::PgPool;

use crate::catalog_docs::TableDocumentation;

/// The text the search slice matches word by word: `namespace.table`, the description,
/// every tag, the owner and the domain, lowercased and space separated.
pub fn search_text(namespace: &str, table: &str, doc: &TableDocumentation) -> String {
    let mut parts = vec![format!("{namespace}.{table}")];
    if let Some(description) = &doc.description {
        parts.push(description.clone());
    }
    parts.extend(doc.tags.iter().cloned());
    if let Some(owner) = &doc.owner {
        parts.push(owner.clone());
    }
    if let Some(domain) = &doc.domain {
        parts.push(domain.clone());
    }
    parts.join(" ").to_lowercase()
}

/// Record (or refresh) the row for one table. **Best effort**: an edit must not fail
/// because the index could not be written; the failure is logged and swallowed.
pub async fn record(
    pool: Option<&PgPool>,
    catalog: &str,
    namespace: &str,
    table: &str,
    doc: &TableDocumentation,
) {
    let Some(pool) = pool else { return };
    let columns = serde_json::to_value(&doc.columns).unwrap_or(serde_json::json!([]));
    let result = sqlx::query(
        "INSERT INTO catalog_doc (catalog, namespace, table_name, description, tags, owner, \
             domain, column_docs, search_text) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (catalog, namespace, table_name) DO UPDATE SET \
             description = EXCLUDED.description, tags = EXCLUDED.tags, owner = EXCLUDED.owner, \
             domain = EXCLUDED.domain, column_docs = EXCLUDED.column_docs, \
             search_text = EXCLUDED.search_text, updated_at = now()",
    )
    .bind(catalog)
    .bind(namespace)
    .bind(table)
    .bind(&doc.description)
    .bind(&doc.tags)
    .bind(&doc.owner)
    .bind(&doc.domain)
    .bind(&columns)
    .bind(search_text(namespace, table, doc))
    .execute(pool)
    .await;
    if let Err(e) = result {
        tracing::warn!(
            "catalog documentation index not updated for {catalog}.{namespace}.{table}: {e}"
        );
    }
}

/// Delete the rows of tables the catalog no longer lists. `present` is the complete
/// `(namespace, table_name)` list of one successful scan; a row for anything else in
/// this catalog is stale. Best effort, like `record`: a scan must not fail because the
/// cleanup did.
pub async fn prune_missing(
    pool: Option<&PgPool>,
    catalog: &str,
    present: &[(String, String)],
) -> u64 {
    let Some(pool) = pool else { return 0 };
    let namespaces: Vec<String> = present.iter().map(|(ns, _)| ns.clone()).collect();
    let tables: Vec<String> = present.iter().map(|(_, table)| table.clone()).collect();
    let result = sqlx::query(
        "DELETE FROM catalog_doc d \
         WHERE d.catalog = $1 \
           AND NOT EXISTS ( \
             SELECT 1 FROM unnest($2::text[], $3::text[]) AS t(ns, tbl) \
             WHERE t.ns = d.namespace AND t.tbl = d.table_name \
           )",
    )
    .bind(catalog)
    .bind(&namespaces)
    .bind(&tables)
    .execute(pool)
    .await;
    match result {
        Ok(done) => done.rows_affected(),
        Err(e) => {
            tracing::warn!("catalog documentation index not pruned for {catalog}: {e}");
            0
        }
    }
}

/// One row of the index, as the search reads it.
#[derive(Debug, Clone)]
pub struct IndexedDoc {
    pub namespace: String,
    pub table: String,
    pub search_text: String,
    pub tags: Vec<String>,
    pub owner: Option<String>,
    pub domain: Option<String>,
}

/// Every indexed row of one catalog. One catalog holds at most a few hundred tables, so
/// the word and filter rules run in Rust where they are unit-tested.
pub async fn rows(pool: Option<&PgPool>, catalog: &str) -> Vec<IndexedDoc> {
    let Some(pool) = pool else { return Vec::new() };
    let found = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            Vec<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT namespace, table_name, search_text, tags, owner, domain \
         FROM catalog_doc WHERE catalog = $1 ORDER BY namespace, table_name",
    )
    .bind(catalog)
    .fetch_all(pool)
    .await;
    match found {
        Ok(rows) => rows
            .into_iter()
            .map(
                |(namespace, table, search_text, tags, owner, domain)| IndexedDoc {
                    namespace,
                    table,
                    search_text,
                    tags,
                    owner,
                    domain,
                },
            )
            .collect(),
        Err(e) => {
            tracing::warn!("catalog documentation index not read for {catalog}: {e}");
            Vec::new()
        }
    }
}

/// Whether one indexed row answers every word and every given filter. `words` are already
/// lowercased; `search_text` is lowercased when it is written.
pub fn row_matches(
    row: &IndexedDoc,
    words: &[String],
    tag: Option<&str>,
    owner: Option<&str>,
    domain: Option<&str>,
) -> bool {
    if let Some(tag) = tag
        && !row.tags.iter().any(|t| t == tag)
    {
        return false;
    }
    if let Some(owner) = owner
        && row.owner.as_deref() != Some(owner)
    {
        return false;
    }
    if let Some(domain) = domain
        && row.domain.as_deref() != Some(domain)
    {
        return false;
    }
    words
        .iter()
        .all(|word| row.search_text.contains(word.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn indexed() -> IndexedDoc {
        let mut doc = doc();
        // A domain is written lowercase (`catalog_docs::domain_ok`).
        doc.domain = Some("sales".into());
        IndexedDoc {
            namespace: "shop".into(),
            table: "daily_revenue".into(),
            search_text: search_text("shop", "daily_revenue", &doc),
            tags: doc.tags.clone(),
            owner: doc.owner.clone(),
            domain: doc.domain.clone(),
        }
    }

    fn words(text: &str) -> Vec<String> {
        text.split_whitespace().map(str::to_lowercase).collect()
    }

    #[test]
    fn a_word_in_the_description_matches() {
        assert!(row_matches(&indexed(), &words("revenue"), None, None, None));
        assert!(row_matches(
            &indexed(),
            &words("daily shop"),
            None,
            None,
            None
        ));
    }

    #[test]
    fn a_tag_filter_rejects_a_row_without_that_tag() {
        assert!(row_matches(
            &indexed(),
            &words("revenue"),
            Some("finance"),
            None,
            None
        ));
        assert!(!row_matches(
            &indexed(),
            &words("revenue"),
            Some("ops"),
            None,
            None
        ));
    }

    #[test]
    fn an_owner_filter_is_exact() {
        assert!(row_matches(
            &indexed(),
            &words("revenue"),
            None,
            Some("analytics"),
            None
        ));
        assert!(!row_matches(
            &indexed(),
            &words("revenue"),
            None,
            Some("analytic"),
            None
        ));
        assert!(!row_matches(
            &indexed(),
            &words("revenue"),
            None,
            Some("Analytics"),
            None
        ));
    }

    #[test]
    fn a_word_in_no_field_is_rejected() {
        assert!(!row_matches(
            &indexed(),
            &words("revenue nothing"),
            None,
            None,
            None
        ));
        assert!(!row_matches(&indexed(), &words("zones"), None, None, None));
    }

    #[test]
    fn a_domain_filter_is_exact_and_lowercase() {
        assert!(row_matches(&indexed(), &[], None, None, Some("sales")));
        assert!(!row_matches(&indexed(), &[], None, None, Some("Sales")));
    }

    fn doc() -> TableDocumentation {
        TableDocumentation {
            description: Some("Daily Revenue per Shop".into()),
            tags: vec!["finance".into(), "daily".into()],
            owner: Some("analytics".into()),
            domain: Some("Sales".into()),
            guide: None,
            columns: Vec::new(),
            can_edit: false,
            edit_rule: String::new(),
            can_rollback: false,
            rollback_rule: String::new(),
        }
    }

    #[test]
    fn search_text_lowercases_every_field() {
        let text = search_text("gold", "daily_revenue", &doc());
        assert_eq!(
            text,
            "gold.daily_revenue daily revenue per shop finance daily analytics sales"
        );
    }

    #[tokio::test]
    async fn record_without_a_pool_is_a_no_op() {
        record(None, "gold", "shop", "daily_revenue", &doc()).await;
    }

    #[tokio::test]
    async fn prune_missing_without_a_pool_is_a_no_op() {
        assert_eq!(prune_missing(None, "gold", &[]).await, 0);
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn prune_missing_deletes_rows_for_vanished_tables_only() {
        let pool = crate::db::test_support::migrated_pool().await;
        record(Some(&pool), "gold", "shop", "daily_revenue", &doc()).await;
        record(Some(&pool), "gold", "shop", "orders", &doc()).await;
        record(Some(&pool), "silver", "shop", "daily_revenue", &doc()).await;

        let deleted = prune_missing(
            Some(&pool),
            "gold",
            &[("shop".to_string(), "daily_revenue".to_string())],
        )
        .await;
        assert_eq!(deleted, 1, "only the vanished table's row");
        let (gold, survivor): (i64, String) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM catalog_doc WHERE catalog = 'gold'), table_name \
             FROM catalog_doc WHERE catalog = 'gold'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(gold, 1);
        assert_eq!(survivor, "daily_revenue");
        let (silver,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM catalog_doc WHERE catalog = 'silver'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(silver, 1, "another catalog is never touched");

        // An empty listing deletes every row of that catalog and none of another.
        let deleted = prune_missing(Some(&pool), "gold", &[]).await;
        assert_eq!(deleted, 1);
        let (gold, silver): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM catalog_doc WHERE catalog = 'gold'), \
                    (SELECT count(*) FROM catalog_doc WHERE catalog = 'silver')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((gold, silver), (0, 1));

        sqlx::query("DELETE FROM catalog_doc")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn record_writes_one_row_and_a_second_call_refreshes_it() {
        let pool = crate::db::test_support::migrated_pool().await;
        record(Some(&pool), "gold", "shop", "daily_revenue", &doc()).await;
        let mut second = doc();
        second.tags.push("monthly".into());
        record(Some(&pool), "gold", "shop", "daily_revenue", &second).await;
        // The ticket's `(array_agg(tags))[1]` reads NULL: `array_agg` of a `text[]` column
        // builds a two-dimensional array, and PostgreSQL answers NULL for a subscript that
        // names fewer dimensions than the array has. The row's own `tags` is the same value.
        let (rows, tags): (i64, Vec<String>) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM catalog_doc WHERE catalog = 'gold'), tags \
             FROM catalog_doc WHERE catalog = 'gold'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rows, 1);
        assert_eq!(
            tags,
            vec![
                "finance".to_string(),
                "daily".to_string(),
                "monthly".to_string()
            ]
        );
    }
}
