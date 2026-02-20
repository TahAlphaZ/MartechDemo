# CRM Platform API Reference

## Salesforce

- **Auth**: OAuth 2.0 (Connected App with access_token + refresh_token)
- **Base URL**: `https://{instance}.salesforce.com/services/data/v59.0`
- **Standard Objects**: Contact, Lead, Opportunity, Account, Task, Campaign, CampaignMember
- **Bulk API**: Bulk API 2.0 for >50k records — `POST /jobs/ingest`
- **Pagination**: `nextRecordsUrl` in response (SOQL query results)
- **Rate limit**: 100,000 API calls/24 hours (Enterprise), 25 concurrent
- **Incremental**: Use `SystemModstamp > {last_sync}` in SOQL WHERE clause
- **Entity mapping**: Opportunity = Deal, Account = Company

## HubSpot

- **Auth**: Private App token (Bearer) or OAuth 2.0
- **Base URL**: `https://api.hubapi.com/crm/v3`
- **Objects**: contacts, companies, deals, engagements, tickets
- **Batch API**: `/objects/{type}/batch/read` for bulk reads (100 per batch)
- **Pagination**: Cursor via `paging.next.after` in response
- **Rate limit**: 100 requests/10 seconds (private app)
- **Incremental**: Use `lastmodifieddate` property filter
- **Note**: HubSpot merges leads into contacts — no separate Lead object

## Microsoft Dynamics 365

- **Auth**: OAuth 2.0 (Azure AD App Registration)
- **Base URL**: `https://{org}.api.crm.dynamics.com/api/data/v9.2`
- **Entities**: contacts, leads, opportunities, accounts, tasks
- **Pagination**: `@odata.nextLink` URL in response
- **Rate limit**: 6000 requests/5 minutes per user
- **Incremental**: Use `$filter=modifiedon gt {datetime}` OData filter
- **Note**: Uses OData v4 protocol

## Zoho CRM

- **Auth**: OAuth 2.0 (Self Client or Server-based)
- **Base URL**: `https://www.zohoapis.com/crm/v6`
- **Modules**: Contacts, Leads, Deals, Accounts, Tasks
- **Pagination**: `page` + `per_page` parameters (max 200 per page)
- **Rate limit**: 2000 API credits/day (Free), 100,000 (Enterprise)
- **Incremental**: Use `Modified_Time` field in criteria
