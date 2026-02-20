# Analytics Platform Reference

## Google Analytics 4 (GA4)

- **Auth**: Service Account (OAuth2 with JSON key)
- **Base URL**: `https://analyticsdata.googleapis.com/v1beta`
- **Key endpoints**: `runReport`, `runRealtimeReport`, `getMetadata`
- **Pagination**: `nextPageToken` in response, pass as `pageToken`
- **Rate limit**: 10 QPS per property
- **Dimensions**: date, sessionSource, sessionMedium, sessionCampaignName, country, deviceCategory
- **Metrics**: sessions, totalUsers, newUsers, conversions, totalRevenue, engagementRate

## Adobe Analytics

- **Auth**: JWT (Service Account) or OAuth2
- **Base URL**: `https://analytics.adobe.io/api/{company_id}`
- **Key endpoints**: `/reports`, `/segments`, `/dimensions`
- **Pagination**: Offset-based (`page` + `limit` params)
- **Rate limit**: 12 requests/second
- **Note**: Uses Report Suite IDs, requires separate `reportSuiteId` per request

## Mixpanel

- **Auth**: Service Account (project secret)
- **Base URL**: `https://data.mixpanel.com/api/2.0`
- **Key endpoints**: `/export` (raw events), `/engage` (user profiles)
- **Pagination**: Streaming JSON lines (no pagination needed for export)
- **Rate limit**: 60 requests/hour for export
- **Note**: Export endpoint returns JSONL (one JSON object per line)

## Amplitude

- **Auth**: API Key + Secret Key (Basic Auth)
- **Base URL**: `https://amplitude.com/api/2`
- **Key endpoints**: `/events/export`, `/usersearch`, `/cohorts`
- **Pagination**: Date-range based (export by date chunks)
- **Rate limit**: 360 requests/hour
- **Note**: Export returns zipped JSON; batch by day for large datasets

## Segment

- **Auth**: Bearer token (Workspace token)
- **Base URL**: `https://profiles.segment.com/v1`
- **Key endpoints**: `/spaces/{id}/collections/users/profiles`
- **Pagination**: Cursor-based (`next` URL in response)
- **Rate limit**: 100 requests/second
- **Note**: Segment is primarily a CDP; pull from Segment warehouses for bulk data
