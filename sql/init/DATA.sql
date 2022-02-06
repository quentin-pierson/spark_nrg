CREATE TABLE D_TEMP(
    Code VARCHAR(255),
    intitule VARCHAR(255),
    Unite VARCHAR(255),
    Region VARCHAR(255),
    Ressource_type VARCHAR(255),
    annee1 FLOAT,
    annee2 FLOAT,
    annee3 FLOAT,
    annee4 FLOAT,
    annee5 FLOAT,
    annee6 FLOAT
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

    MERGE D_REGION AS DST
    USING (SELECT DISTINCT Region AS LB_REGION 
            FROM D_TEMP AS TMP)
        AS SRC
        ON DST.Libelle = SRC.LB_REGION
    WHEN NOT MATCHED THEN
    INSERT (LB_REGION)
    VALUES (SRC.LB_REGION)
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