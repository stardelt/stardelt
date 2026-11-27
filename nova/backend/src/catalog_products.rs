//! The data products of one catalog (stardelt/stardelt#37, slice 3): every documented table
//! that carries a `stardelt.lifecycle`, read from the documentation index
//! (`catalog_doc_store`). The index is written on every documentation edit through Nova, so
//! a lifecycle set there appears at once; an edit made directly in Trino or Lakekeeper does
//! not refresh it until the next Nova edit of that table — the staleness catalog search has.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;
use crate::catalog_doc_store::DataProduct;

/// `GET /api/catalog/products` query.
#[derive(Debug, Default, Deserialize)]
pub struct ProductsQuery {
    pub catalog: Option<String>,
    pub domain: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DataProducts {
    pub catalog: String,
    pub products: Vec<DataProduct>,
}

#[utoipa::path(
    get,
    operation_id = "catalog_products",
    path = "/api/catalog/products",
    tag = "public",
    description = "The data products of one catalog (#37): every documented table that carries `stardelt.lifecycle` (`draft`, `published` or `deprecated`), with its owner, domain, description and replacement. `?domain=` narrows to one domain, exactly and case-sensitively. The answer comes from Nova's documentation index, which is written on every documentation edit through Nova; an edit made directly in the catalog does not refresh it until the next Nova edit of that table. At most 500 products, in domain order (tables without a domain last), then namespace and table.",
    params(
        ("catalog" = Option<String>, Query, description = "The catalog to read; omitted, the default one"),
        ("domain" = Option<String>, Query, description = "Only products of this `stardelt.domain`"),
    ),
    responses(
        (status = 200, description = "The catalog's data products", body = DataProducts),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list(
    State(s): State<Arc<AppState>>,
    caller: Option<axum::Extension<crate::auth::Principal>>,
    Query(params): Query<ProductsQuery>,
) -> Response {
    let caller = caller.as_ref().map(|c| &c.0);
    let (catalog, _id) =
        match crate::catalog::catalog_scope(&s, caller, params.catalog.as_deref()).await {
            Ok(scope) => scope,
            Err(response) => return response,
        };
    let Some(pool) = s.db.as_ref() else {
        return crate::saved_queries::unavailable();
    };
    let domain = params
        .domain
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty());
    match crate::catalog_doc_store::products(pool, &catalog, domain).await {
        Ok(products) => Json(DataProducts { catalog, products }).into_response(),
        Err(e) => crate::saved_queries::store_failed(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_product_serializes_with_the_names_the_page_reads() {
        let answer = DataProducts {
            catalog: "gold".into(),
            products: vec![DataProduct {
                namespace: "shop".into(),
                table_name: "daily_revenue".into(),
                description: None,
                owner: Some("demo".into()),
                domain: Some("finance".into()),
                lifecycle: "published".into(),
                replaced_by: None,
            }],
        };
        let json = serde_json::to_value(&answer).expect("the answer serializes");
        assert_eq!(
            json["products"][0]["table"],
            serde_json::json!("daily_revenue")
        );
        assert_eq!(json["products"][0]["domain"], serde_json::json!("finance"));
        assert_eq!(json["products"][0]["replacedBy"], serde_json::Value::Null);
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn products_come_from_the_index_with_their_lifecycle() {
        let pool = crate::db::test_support::migrated_pool().await;
        let docs =
            |lifecycle: Option<&str>, domain: &str| crate::catalog_docs::TableDocumentation {
                description: Some("a table".into()),
                tags: Vec::new(),
                owner: Some("demo".into()),
                domain: Some(domain.into()),
                guide: None,
                lifecycle: lifecycle.map(str::to_string),
                replaced_by: None,
                columns: Vec::new(),
                can_edit: false,
                edit_rule: String::new(),
                can_rollback: false,
                rollback_rule: String::new(),
            };
        crate::catalog_doc_store::record(
            Some(&pool),
            "gold",
            "shop",
            "orders",
            &docs(Some("published"), "sales"),
        )
        .await;
        crate::catalog_doc_store::record(
            Some(&pool),
            "gold",
            "shop",
            "tickets",
            &docs(None, "ops"),
        )
        .await;
        let found = crate::catalog_doc_store::products(&pool, "gold", None)
            .await
            .expect("the index reads");
        assert_eq!(
            found.len(),
            1,
            "a table without a lifecycle is not a product"
        );
        assert_eq!(
            (found[0].table_name.as_str(), found[0].lifecycle.as_str()),
            ("orders", "published")
        );
        let finance = crate::catalog_doc_store::products(&pool, "gold", Some("finance"))
            .await
            .expect("the index reads");
        assert!(finance.is_empty(), "the domain filter is exact");
    }
}
