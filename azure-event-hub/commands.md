### azure streaming
1. create event hub - create event - create shared policy (send/create)
2. configure event producer at source
3. streaming analytics - create job - configure consumer using no-code-editor or use query
4. Azure Data Lake Storage Gen2
5. Query - Data Dump
   ```sql
    select *
    into "temperature-event"
    from "ng-sensor-emitter"
   ```