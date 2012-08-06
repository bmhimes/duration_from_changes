/* Starting state rows */
SELECT
  /* Simply a unique identifier. */
  CONCAT(entity.id, '-state-0') 'Timeline ID',
  /* Matches the fact to the relevant entity */
  entity.id 'Entity ID',
  /* The value of the slowly changing dimension */
  COALESCE(
    valuechange.oldvalue,
    entity.state
  ) 'State Timeline',
  /* The timestamp that the dimension took the value */
  entity.created 'State Timeline Start Timestamp',
  /* The date that the dimension took on the value, performed as part of
  the ETL to save calculating it over and over in the in-memory 
  analytics application. */
  DATE(entity.created) 'State Timeline Start Date',
  /* The stimestamp that the dimension stopped having the value, 
  indicated by a change to a new value, the resolution of the entity,
  or, if the entity was closed without being resolved, the close time of
  the entity. */
  COALESCE(
    dimensionchanges.created,
    entity.resolutiondate,
    latest_status.close_timestamp
  ) 'State Timeline End Timestamp',
  /* The date that the dimension stopped having the value, indicated by
  a change to a new value, the resolution of the entity, or, if the
  entity was closed without being resolved, the close time of the 
  entity.  This is perfomed as part of the ETL to save calculating it 
  over and over in the in-memory analytics application. */
  COALESCE(
    DATE(dimensionchanges.created),
    DATE(entity.resolutiondate),
    DATE(latest_status.close_timestamp)
  ) 'State Timeline End Date',
  /* The duration that the dimension had the value, calculated by 
  subtracing the start timestamp from the end timestamp.  This is 
  performed as part of the ETL to save calculating it over and over in 
  the in-memory analytics application. */
  TIMESTAMPDIFF(
    SECOND,
    entity.created,
    COALESCE(
      dimensionchanges.created,
      entity.resolutiondate,
      latest_status.close_timestamp
    )
  ) / 3600 'State Timeline Duration'
FROM
  /* Start with the entitys, since all entitys have an state */
  entity
  /* Bring in th enext change to state */
  LEFT OUTER JOIN
    (SELECT
      entity.id 'entityid',
      MIN(valuechange.id) 'valuechangeid'
    FROM
      entity
      INNER JOIN dimensionchanges
        ON entity.id = dimensionchanges.entityid
      INNER JOIN valuechange
        ON dimensionchanges.id = valuechange.changesid
        AND valuechange.dimension = 'state'
    GROUP BY entity.id) first_state_change
    ON entity.id = first_state_change.entityid
  /* Bring in the data of the next change to state */
  LEFT OUTER JOIN valuechange
    ON first_state_change.valuechangeid = valuechange.id
  /* Bring in the timing data of the next change to state */
  LEFT OUTER JOIN dimensionchanges
    ON valuechange.changesid = dimensionchanges.id
  /* Bring in the time the entity was closed, if it is currently closed
  */
  LEFT OUTER JOIN
    (SELECT
      dimensionchanges.entityid,
      dimensionchanges.created 'close_timestamp'
    FROM
      (SELECT
        dimensionchanges.entityid,
        MAX(dimensionchanges.id) 'changesid'
      FROM
        dimensionchanges
        INNER JOIN valuechange
          ON dimensionchanges.id = valuechange.changesid
          AND valuechange.dimension = 'status'
      GROUP BY dimensionchanges.entityid) latest_status
      INNER JOIN dimensionchanges
        ON latest_status.changesid = dimensionchanges.id
      INNER JOIN valuechange
        ON dimensionchangeslid = valuechange.changesid
        AND valuechange.dimension = 'status'
        AND valuechange.newstring = 'closed') latest_status
    ON entity.id = latest_status.entityid
UNION
/* Change rows, excluding starting value */
SELECT
  /* Simply a unique modifier */
  CONCAT(
    entity.id,
    '-state-',
    dimensionchanges.id
  ) 'Timeline ID',
  /* Matches the fact to the relevant entity */
  entity.id 'Entity ID',
  /* The value of the slowly changing dimension */
  valuechange.newvalue 'State Timeline',
  /* The timestamp that the dimension took on the value */
  changegropu.created 'State Timeline Start Timestamp',
  /* The date that the dimension took on th evalue, performed as part
  of the ETL to save calculating it over and over in the in-memory 
  analytics application */
  DATE(dimensionchanges.created) 'State Timeline Start Date',
  /* The timestamp that the dimension stopped having the value,
  indicated by a change to a new value, the resolution of the entity,
  or, if the entity was closed without being resolved, the close time of
  the entity. */
  COALESCE(
    next_changes.created,
    entity.resolutiondate,
    latest_status.close_timestamp
  ) 'State Timeline End Timestamp',
  /* The date that the dimension stopped having the value, indicated by 
  a change to a new value, the resolution of the entity, or, if the 
  entity was closed without being resolved, the close time of the 
  entity.  This is performed as part of the ETL  to save calculating it 
  over and over in the in-memory analytics application. */
  COALESCE(
    DATE(nextchanges.created),
    DATE(entity.resolutiondate),
    DATE(latest_status.close_timestamp)
  ) 'State Timeline End Date',
  /* The duration that the dimension had the value, calculated by 
  subtracting the start timestamp from the end timestamp. This is 
  performed as part of the ETL to save calculating it over and over in 
  the in-memory analytics application. */
  TIMESTAMPDIFF(
    SECOND,
    dimensionchanges.created,
    COALESCE(
      next_changes.created,
      entity.resolutiondate,
      latest_status.close_timestamp
    )
  ) / 3600 'State Timeline Duration'
FROM
  /* Start with changes to state, since we already have the first rows 
  above */
  dimensionchanges
  INNER JOIN valuechange
    ON dimensionchanges.id = valuechange.changesid
    AND valuechange.dimension = 'state'
  /* Find the next change to state */
  LEFT OUTER JOIN
    (SELECT
      dimensionchanges.id,
      MIN(later_changes.id) 'changesid'
    FROM
      dimensionchanges
      INNER JOIN valuechange
        ON dimensionchanges.id = valuechange.changesid
        AND valuechange.dimension = 'state'
      INNER JOIN dimensionchanges later_changes
        ON dimensionchanges.entityid = later_changes.entityid
        AND dimensionchanges.created < later_changes.created
      INNER JOIN valuechange later_state
        ON later_changes.id = later_state.changesid
        AND later_state.dimension = 'state'
    GROUP BY dimensionchanges.id) next_changes_filter
    ON dimensionchanges.id = next_changes_filter.id
  /* Bring in the timing data on the change to state */
  LEFT OUTER JOIN dimensionchanges next_changes
    ON next_changes_filter.changesid = next_changes.id
  /* Bring in the entity for resolutiontime */
  LEFT OUTER JOIN entity
    ON dimensionchanges.entityid = entity.id
  /* Bring in the time the entity was closed, if it is currently closed 
  */
  LEFT OUTER JOIN 
    (SELECT
      dimensionchanges.entityid,
      dimensionchanges.created 'close_timestamp'
    FROM
      (SELECT
        dimensionchanges.entityid,
        MAX(dimensionchanges.id) 'changesid'
      FROM
        dimensionchanges
        INNER JOIN valuechange
          ON dimensionchanges.id = valuechange.changesid
          AND valuechange.dimension = 'status'
      GROUP BY dimensionchanges.entityid) latest_status
      INNER JOIN dimensionchanges
        ON latest_status.changesid = dimensionchanges.id
      INNER JOIN valuechange
        ON dimensionchanges.id = valuechange.changesid
        AND valuechange.dimension = 'status'
        AND valuechange.newstring = 'closed') latest_status
    ON entity.id = latest_status.entityid
