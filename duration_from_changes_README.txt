This query was designed under the conditions that temp tables were not
to be used.  Its purpose is to transform a series of point-in-time
changes to a slowly changing dimension into a series of values and
durations.

The basic schema is that there is an entity.  This entity has a state
and a status.  Its status can be "Open", "Resolved", and "Closed".  The
entity has multiple slowly-changing dimensions, but we will only
consider the "state" dimension.  Whenever one or more slowly-changing 
dimensions are changed, a record is made once in the dimensionchanges 
table, and a record is created in valuechange for each dimension that 
was changed at that moment.  Thus, there is a record at each point in
time that a slowly-changing dimension changes.

This query transforms those point-in-time records into duration records,
indicating the total time that a dimension took on a particular value.
This is useful for performing statistical analysis, including looking at
average duration per "state" value, identifying bottlenecks in changes
to state, and counting the average number of unique states encountered.
