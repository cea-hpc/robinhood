DROP FUNCTION IF EXISTS one_path;
DELIMITER //
CREATE FUNCTION one_path(param VARCHAR(128)) RETURNS VARCHAR(1024) READS SQL DATA
BEGIN
    DECLARE p VARCHAR(1024) DEFAULT NULL;
    DECLARE pid VARCHAR(128) DEFAULT NULL;
    DECLARE n VARCHAR(256) DEFAULT NULL;
    -- return '/p' when not found
    DECLARE EXIT HANDLER FOR NOT FOUND RETURN p;
    SELECT parent_id, name INTO pid, p from NAMES where id=param LIMIT 1;
    LOOP
        SELECT parent_id, name INTO pid, n from NAMES where id=pid ;
        SELECT CONCAT( n, '/', p) INTO p;
    END LOOP;
END//

DROP FUNCTION IF EXISTS this_path;
DELIMITER //
CREATE FUNCTION this_path(pid_arg VARCHAR(128), n_arg VARCHAR(256)) RETURNS VARCHAR(1024) READS SQL DATA
BEGIN
    DECLARE p VARCHAR(1024) DEFAULT NULL;
    DECLARE pid VARCHAR(128) DEFAULT NULL;
    DECLARE n VARCHAR(256) DEFAULT NULL;
    -- return '/p' when not found
    DECLARE EXIT HANDLER FOR NOT FOUND RETURN p;
    SET pid=pid_arg;
    SET p=n_arg;
    LOOP
        SELECT parent_id, name INTO pid, n from NAMES where id=pid ;
        SELECT CONCAT( n, '/', p) INTO p;
    END LOOP;
END//

DROP PROCEDURE IF EXISTS all_paths;
CREATE PROCEDURE all_paths(IN param VARCHAR(128)) READS SQL DATA
BEGIN
    DECLARE p VARCHAR(1024) DEFAULT '';
    DECLARE pid VARCHAR(128) DEFAULT '';
    DECLARE n VARCHAR(256) DEFAULT '';
    DECLARE nb INT;
    DECLARE cur CURSOR FOR SELECT parent_id, name from NAMES where id=param;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET nb=0;
    OPEN cur;
    -- loop on all hardlinks to an object
    alllinks: LOOP 
        SET nb=1;
        FETCH cur INTO pid, p;
--        SELECT nb, pid, p;
        IF nb=0 THEN 
--            SELECT 'EXIT' as 'MSG:';
            LEAVE alllinks;
        END IF;
        WHILE nb > 0 DO
            SELECT parent_id, name INTO pid, n from NAMES where id=pid LIMIT 1;
            SELECT FOUND_ROWS() INTO nb;
            IF nb > 0 THEN
                SELECT CONCAT( n, '/', p) INTO p;
            END IF;
        END WHILE;
        SELECT p AS paths;

    END LOOP alllinks;
    CLOSE cur;

END//

SELECT one_path(id), type, size from ENTRIES WHERE id='286A47DD:1000D';
SELECT one_path('toto');
SELECT one_path('286A47DD:1000D');

-- hardlink: 286A47DD:1000D
CALL all_paths('286A47DD:1000D');
