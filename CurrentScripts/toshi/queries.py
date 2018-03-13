org_update = """INSERT IGNORE INTO 
                OrganizationStateAffiliation (oid,state) 
                select distinct oid,state 
                from CombinedRepresentations 
                where oid is not null and state is not null 
                and oid not in 
                (select oid 
                from Organizations 
                where name not RLIKE \'^[a-zA-Z0-9]\')"""