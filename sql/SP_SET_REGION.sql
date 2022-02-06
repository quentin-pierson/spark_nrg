DELIMITER //
CREATE PROCEDURE SP_SET_REGION()
    BEGIN

    INSERT INTO D_REGION 
    (Libelle)
    SELECT t.Region  
    FROM D_TEMP t
    WHERE NOT EXISTS(SELECT Libelle
                    FROM D_REGION r
                    WHERE r.Libelle = t.Region);
    END //
DELIMITER ;