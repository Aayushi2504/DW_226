WITH src_user_session_channel AS (
    SELECT userId, sessionId, channel
    FROM {{ source('raw_data', 'user_session_channel') }}
    WHERE sessionId IS NOT NULL
)
SELECT * FROM src_user_session_channel
