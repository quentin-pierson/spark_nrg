DELIMITER //
CREATE PROCEDURE SP_SET_RESSOURCE(
    BEGIN

    INSERT INTO D_RESSOURCE (Unite_Energetique, Valeur_Energetique, DT_Year, 
    FK_Type_Ressource, FK_Region, FK_Secteur_Energetique)
    SELECT t.Unite, t.annee1, '2014-01-01', tr.ID_Type_Ressource, r.ID_Region, se.Code_Secteur_Energetique
    FROM D_TEMP AS t
    INNER JOIN D_REGION AS r ON t.Region = r.Libelle
    INNER JOIN D_TYPE_RESSOURCE AS tr ON t.Ressource_type = tr.Libelle
    INNER JOIN D_REGION AS se ON t.Code = se.Code_Secteur_Energetique;

    END //
DELIMITER ;