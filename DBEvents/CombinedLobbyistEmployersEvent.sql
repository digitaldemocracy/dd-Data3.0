/*
  Used for diplaying 'Known Employers' on the site.
  Note: Simple because there is no dr_id
*/

DROP EVENT IF EXISTS CombinedLobbyistEmployers_event;

DELIMITER |


CREATE EVENT CombinedLobbyistEmployers_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN

    DROP TABLE IF EXISTS CombinedLobbyistEmployers;

    CREATE TABLE IF NOT EXISTS CombinedLobbyistEmployers (
      pid INT,
      assoc_name VARCHAR(255),
      rpt_date DATE,
      rpt_date_ts INT,
      ls_beg_yr YEAR,
      ls_end_yr YEAR,
      state VARCHAR(2),

      INDEX pid_idx (pid),
      INDEX state_idx (state),
      INDEX ls_beg_yr_idx (ls_beg_yr),
      INDEX ls_end_yr_idx (ls_end_yr)
    );

    INSERT INTO CombinedLobbyistEmployers
      SELECT le.pid,
        lf.filer_naml as assoc_name,
        le.rpt_date,
        le.rpt_date_ts,
        le.ls_beg_yr,
        le.ls_end_yr,
        le.state
      FROM LobbyistEmployment le
        JOIN LobbyingFirmState lf
          ON le.sender_id = lf.filer_id
             AND le.state = lf.state
      UNION
      SELECT le.pid,
        o.name as assoc_name,
        le.rpt_date,
        le.rpt_date_ts,
        le.ls_beg_yr,
        le.ls_end_yr,
        le.state
      FROM LobbyistDirectEmployment le
        JOIN Organizations o
          ON le.lobbyist_employer = o.oid;

  END |

DELIMITER ;
