CREATE TABLE D_TEMP(
    Production VARCHAR(255),
    intitule VARCHAR(255),
    Unite VARCHAR(255),
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
    ID_Secteur_Energetique INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
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
    Libelle VARCHAR(255) NOT NULL,
    Unite_Energetique VARCHAR(255) NOT NULL,
    Valeur_Energetique FLOAT NOT NULL,
    DT_Year DATE NOT NULL,
    FK_Type_Ressource INT NOT NULL,
    FK_Region INT NOT NULL,
    FK_Secteur_Energetique INT NOT NULL,
    DELETE_LINE INT DEFAULT 0,
    DT_INSERT TIMESTAMP DEFAULT now(),
    DT_UPDATE TIMESTAMP DEFAULT now(),
    FOREIGN KEY (DT_Year) REFERENCES D_DATE(DT_Year),
    FOREIGN KEY (FK_Type_Ressource) REFERENCES D_TYPE_RESSOURCE(ID_Type_Ressource),
    FOREIGN KEY (FK_Region) REFERENCES D_REGION(ID_Region),
    FOREIGN KEY (FK_Secteur_Energetique) REFERENCES D_SECTEUR_ENERGETIQUE(ID_Secteur_Energetique)
);

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