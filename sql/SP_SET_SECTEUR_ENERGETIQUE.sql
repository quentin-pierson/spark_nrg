DELIMITER //
CREATE PROCEDURE SP_SET_SECTEUR_ENERGETIQUE()
    BEGIN

    INSERT INTO D_SECTEUR_ENERGETIQUE 
    (Code_Secteur_Energetique, Libelle)
    SELECT t.Code, t.Intitule  
    FROM D_TEMP t
    WHERE NOT EXISTS(SELECT Libelle
                    FROM D_SECTEUR_ENERGETIQUE se
                    WHERE se.Code_Secteur_Energetique = t.Code);
    END //
DELIMITER ;