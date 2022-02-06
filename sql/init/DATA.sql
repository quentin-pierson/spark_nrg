CREATE TABLE D_TEMP(
    Code VARCHAR(255),
    Intitule VARCHAR(255),
    Unite VARCHAR(255),
    Region VARCHAR(255),
    Ressource_type VARCHAR(255),
    Annee1 FLOAT,
    Annee2 FLOAT,
    Annee3 FLOAT,
    Annee4 FLOAT,
    Annee5 FLOAT,
    Annee6 FLOAT
);

CREATE TABLE D_DATE(
    DT_Year DATE NOT NULL PRIMARY KEY,
    Libelle_Year VARCHAR(4) NOT NULL
);

CREATE TABLE D_REGION(
    ID_Region INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Libelle VARCHAR(255) NOT NULL,
    DELETE_LINE INT DEFAULT 0,
    DT_INSERT TIMESTAMP DEFAULT now(),
    DT_UPDATE TIMESTAMP DEFAULT now()
);

CREATE TABLE D_SECTEUR_ENERGETIQUE(
    Code_Secteur_Energetique VARCHAR(255) PRIMARY KEY NOT NULL,
    Libelle VARCHAR(255) NOT NULL,
    DELETE_LINE INT DEFAULT 0,
    DT_INSERT TIMESTAMP DEFAULT now(),
    DT_UPDATE TIMESTAMP DEFAULT now()
);

CREATE TABLE D_TYPE_RESSOURCE(
    ID_Type_Ressource INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Libelle VARCHAR(255) NOT NULL,
    DELETE_LINE INT DEFAULT 0,
    DT_INSERT TIMESTAMP DEFAULT now(),
    DT_UPDATE TIMESTAMP DEFAULT now()
);

CREATE TABLE D_RESSOURCE(
    ID_Ressource INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Unite_Energetique VARCHAR(255) NULL,
    Valeur_Energetique FLOAT NOT NULL,
    DT_Year DATE NOT NULL,
    FK_Type_Ressource INT NOT NULL,
    FK_Region INT NOT NULL,
    FK_Secteur_Energetique VARCHAR(255) NOT NULL,
    DELETE_LINE INT DEFAULT 0,
    DT_INSERT TIMESTAMP DEFAULT now(),
    DT_UPDATE TIMESTAMP DEFAULT now(),
    FOREIGN KEY (DT_Year) REFERENCES D_DATE(DT_Year),
    FOREIGN KEY (FK_Type_Ressource) REFERENCES D_TYPE_RESSOURCE(ID_Type_Ressource),
    FOREIGN KEY (FK_Region) REFERENCES D_REGION(ID_Region),
    FOREIGN KEY (FK_Secteur_Energetique) REFERENCES D_SECTEUR_ENERGETIQUE(Code_Secteur_Energetique)
);

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

DELIMITER //
CREATE PROCEDURE SP_SET_RESSOURCE()
    BEGIN

    INSERT INTO D_RESSOURCE 
    (`Unite_Energetique`, `Valeur_Energetique`, `DT_Year`, `FK_Type_Ressource`, `FK_Region`, `FK_Secteur_Energetique`)
    SELECT t.Unite, t.Annee1, '2014-01-01', tr.ID_Type_Ressource, r.ID_Region, se.Code_Secteur_Energetique
    FROM D_TEMP AS t
    INNER JOIN D_REGION AS r ON t.Region = r.Libelle
    INNER JOIN D_TYPE_RESSOURCE AS tr ON t.Ressource_type = tr.Libelle
    INNER JOIN D_REGION AS se ON t.Code = se.Code_Secteur_Energetique;

    END //
DELIMITER ;

INSERT INTO D_DATE (`DT_Year`, `Libelle_Year`) VALUES ('2014-01-01', '2014'),
                        ('2015-01-01', '2015'),
                        ('2016-01-01', '2016'),
                        ('2017-01-01', '2017'),
                        ('2018-01-01', '2018'),
                        ('2019-01-01', '2019'),
                        ('2020-01-01', '2020');

INSERT INTO D_REGION (`Libelle`) VALUES ('Auvergne Rhone Alpes'),
                                    ('Bourgogne Franche Comte'),
                                    ('Bretagne'),
                                    ('Centre val de Loire'),
                                    ('Corse'),
                                    ('France Metropolitaine'),
                                    ('Grand Est'),
                                    ('Hauts de France'),
                                    ('Ile de France'),
                                    ('Normandie'),
                                    ('Nouvelle Aquitaine'),
                                    ('Occitanie'),
                                    ('Pays de la Loire'),
                                    ('Provence Alpes Cote d Azur');

INSERT INTO D_TYPE_RESSOURCE (`Libelle`) VALUES ('PRODUCTION'),
                                            ('CONSOMMATION');