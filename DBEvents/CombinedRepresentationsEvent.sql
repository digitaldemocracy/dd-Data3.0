DROP EVENT IF EXISTS CombinedRepresentations_event;

DELIMITER |

CREATE EVENT CombinedRepresentations_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN
    DROP TABLE CombinedRepresentations;

    CREATE TABLE IF NOT EXISTS CombinedRepresentations (
      pid INT,
      hid INT,
      did INT,
      oid INT,
      state VARCHAR(2),
      year YEAR,
      lastTouched TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      dr_id INT AUTO_INCREMENT,

      UNIQUE (dr_id),

      FOREIGN KEY (pid) REFERENCES Person(pid),
      FOREIGN KEY (hid) REFERENCES Hearing(hid),
      FOREIGN KEY (did) REFERENCES BillDiscussion(did),
      FOREIGN KEY (oid) REFERENCES Organizations(oid),
      FOREIGN KEY (state) REFERENCES State(abbrev),

      INDEX year_idx (year),
      INDEX pid_idx (pid),
      INDEX hid_idx (hid),
      INDEX did_idx (did),
      INDEX oid_idx (oid),
      INDEX state_idx (state)
    )
    ENGINE = INNODB
    CHARACTER SET utf8 COLLATE utf8_general_ci;

    CREATE OR REPLACE VIEW CombinedRepresentationsView
      AS
      SELECT
        pid,
        h.hid,
        did,
        oid,
        gp.state,
        year(h.date) AS year
      FROM GeneralPublic gp
        JOIN Hearing h
          ON gp.hid = h.hid
      UNION
      SELECT
        pid,
        h.hid,
        did,
        oid,
        lr.state,
        year(h.date) AS year
      FROM LobbyistRepresentation lr
        JOIN Hearing h
          ON lr.hid = h.hid;

    INSERT INTO CombinedRepresentations
    (pid, hid, did, oid, state, year)
    SELECT v.*
    FROM CombinedRepresentationsView v
      LEFT JOIN CombinedRepresentations t
      ON v.pid = t.pid
        AND v.did = t.did
        AND v.oid = t.oid
    WHERE t.pid IS NULL
      AND t.did IS NULL
      AND t.oid IS NULL
      AND v.oid IS NOT NULL;

    delete t from CombinedRepresentations t
      left join CombinedRepresentationsView v
        ON v.pid = t.pid
           AND v.did = t.did
           AND v.oid = t.oid
    WHERE v.pid IS NULL
          AND v.did IS NULL
          AND v.oid IS NULL;

    DROP VIEW CombinedRepresentationsView;

    END |

DELIMITER ;
