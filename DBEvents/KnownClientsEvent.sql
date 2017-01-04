# Known clients view for Kristian based on Lobbying Contracts
drop table KnownClients;
CREATE TABLE KnownClients
AS
  select distinct le.pid,
    o.name as assoc_name,
    o.oid,
    year(lc.rpt_date) as year,
    le.state
  from LobbyistEmployment le
    join LobbyingContracts lc
      on lc.filer_id = le.sender_id
         and lc.state = le.state
    join Organizations o
      on lc.lobbyist_employer = o.oid;

alter table KnownClients
  add UNIQUE (pid, assoc_name, oid, year, state);

alter table KnownClients
  add index pid_idx (pid);

alter table KnownClients
  add index oid_idx (oid);
