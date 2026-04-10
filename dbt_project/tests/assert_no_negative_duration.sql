-- assert_no_negative_duration.sql
-- Expects 0 rows.
-- fct_sessions filters out is_negative_duration sessions from int_sessions.
-- Any row here means the mart filter is broken.

select session_id
from {{ ref('fct_sessions') }}
where duration_minutes < 0
