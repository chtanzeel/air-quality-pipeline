# API probe findings (2026-10-14)

Real calls to `https://air-quality-api.open-meteo.com/v1/air-quality` with `domains=cams_global`,
`timezone=GMT`. Raw responses and their request parameters are in `tests/fixtures/`.

## Response shape

- One location: a JSON object. Several comma-separated locations: a JSON **list**, in request order.
- `location_id` appears on list items 1..n but is **missing on item 0**, so items are mapped to
  cities by list position, not by `location_id`.
- `hourly.time` is `yyyy-mm-ddThh:mm` with no offset; with `timezone=GMT`, `utc_offset_seconds` is 0.
- `hourly_units`: all pollutants and dust in `μg/m³` (carbon monoxide too, not ppm), `us_aqi` in `USAQI`.
- Coordinates snap to a grid cell. Each city gets a distinct cell:

  | city | requested | returned | elevation (m) |
  |---|---|---|---|
  | lhr | 31.5204, 74.3587 | 31.5, 74.4 | 220 |
  | khi | 24.8607, 67.0011 | 24.9, 67.0 | 7 |
  | isb | 33.6844, 73.0479 | 33.7, 73.0 | 527 |
  | pew | 34.0151, 71.5249 | 34.0, 71.5 | 349 |
  | fsd | 31.4504, 73.1350 | 31.5, 73.1 | 188 |

## Time coverage

- Data starts at **2022-08-04T00:00 UTC**, not 2022-08-01. Earlier hours come back as `null`.
- `us_aqi` starts at **2022-08-05T00:00**, because it needs 24 hours of PM history.
- Dates before coverage (e.g. 2021) return **HTTP 200 with every value null**, not an error.
- The response includes forecast hours: about 5 days ahead (57 future hours in the multi-city
  fixture at fetch time 14:19Z). These must be dropped at load.

## Temporal resolution

- The docs list CAMS Global as **3-hourly**, but the API returns a value for every hour.
  The docs don't say how the in-between hours are filled.
- Empirically (Lahore, 2026-09-01..10), the in-between hours are **not linear interpolation**.
  Almost none sit on the straight line between the neighbouring 3-hourly values, and 16–34%
  fall outside the range of those two values. The second difference spikes at hours divisible
  by 3, i.e. the curve has kinks at the native steps. That fits a smooth (cubic-style)
  interpolation between 3-hourly model steps. Treat 2 of every 3 hours as derived, not native.

## AQI and window independence

- US AQI uses rolling means (PM 24 h, CO 8 h, O3 8 h / hourly, SO2 24 h / hourly, NO2 hourly).
- Overlapping windows (09-01..10 vs 09-05..10) return **identical** values for every variable,
  including `us_aqi` at the window start. So the rolling means use server-side history, not just
  the requested window, and loading in chunks is safe.

## Errors

- HTTP 400 with `{"error": true, "reason": "..."}` as documented. The reason can be
  vague: inverted dates give only `"Bad Request"`, while an invalid variable gives a long internal
  type message.

## Terms and rate limits (open-meteo.com/en/terms, /en/pricing)

- Free tier is non-commercial only: 600 calls/min, 5,000/hour, 10,000/day, 300,000/month.
- Calls are weighted: more than 10 variables, or more than 2 weeks for a single location, counts
  as more than one call (fractional). Multi-location requests are not currently said to count extra.
  A full backfill at 31-day windows × 5 cities (~50 months) is ~250 requests, roughly 550 weighted
  calls, well under the daily limit.
- Data licence: CC BY 4.0. Open-Meteo citation: Zippenfenig, P. (2023). *Open-Meteo.com Weather
  API* [Computer software]. Zenodo. https://doi.org/10.5281/ZENODO.7970649

## Open question: CAMS attribution

The only citation on the air quality docs page is for **CAMS European air quality forecasts,
ENSEMBLE data**, which is a different product from the CAMS Global forecasts used here. The
licence page doesn't mention CAMS. The correct global-product attribution needs to come from the
Copernicus/CAMS licence before the README is written.

## Decisions taken from these findings

1. Default backfill start date is **2022-08-04** (first hour with data), not 2022-08-01.
2. Rows where **every** measure is null (before coverage, or gaps) are dropped at load.
   Partly null rows are kept, e.g. `us_aqi` is null on 2022-08-04 while PM values exist.
3. Staging adds an **`is_native_step`** flag (UTC hour divisible by 3), so 3-hourly model
   output can be told apart from the API's interpolated hours.
4. **One request per city per window**, matching the `landing/<city_id>/` layout and keeping
   retries and lineage per city. This avoids relying on list position in multi-location responses.
