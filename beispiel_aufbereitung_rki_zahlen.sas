Options source source2 notes fullstimer mprint msglevel=i vnferr merror serror sql_ip_trace=(note,source);

/***************************************************************************************************************************/
/* globale Festlegungen */
/* Einlese-/Ausgabe-Pfad */
%let mypath = \\cc1864\dfs\3p\80_DAV\AGTarifierung\2021\betroffenheit\daten\;

/* Quellangaben zu den Dateien stehen in den jeweiligen Einlesebereichen, */
/* Dateien wurden jeweils heruntergeladen */
%let in_rki_covid19_zr  = rki_covid19.csv; /* Zeitreihe zu den Fallzahlen nach Landkreis/Altersgruppe/Geschlecht */
%let in_rki_covid19_akt = RKI_Corona_Landkreise.csv;  /* aktuelle Fallzahlen nah Landkreis */
%let in_rki_impfzahlen  = Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv; /* Zeitreihe Impfzahlen nach Landkreis/Alter/Impfung */
%let in_mobilitaet_lkr  = mobility_change_kreise.csv; /* Zeitreihe Mobilitätsänderung nach Landkreis */
%let in_mobilitaet_bl   = mobility_change_bundeslaender.csv; /* Zeitreihe Mobilitätsänderung nach Bundesland */
%let in_mobilitaet_ges  = mobility_change_germany.csv; /* Zeitreihe Mobilitätsänderung nach Gesamt-Deutschland */
%let in_schl_nuts       = pc2020_DE_NUTS-2021_v3.0.csv; /* Definition NUTS */
%let in_schl_plz_kgs    = zuordnung_plz_ort_landkreis.csv; /* Definition amtlicher Gemeindeschlüssel */
%let in_divi            = zeitreihe-tagesdaten.csv; /* Zeitreihe DIVI-Register nach Landkreis */
%let in_hospital        = Aktuell_Deutschland_COVID-19-Hospitalisierungen.csv; /* Zeitreihe Hospitalisierung nach Bundesland */

%let out_landkreis      = tab_betroffenheit_landkreis;
%let out_bundesland     = tab_betroffenheit_bundesland;
%let out_gesamt         = tab_betroffenheit_deutschland;

%let lib_work           = work;   /* interner Arbeitsbereich */
%let lib_ziel           = schl_db; /* finaler Ablageort */

/* Kennzeichen, ob Makro laufen soll: 1 = Ja, 0 = Nein */ 
%let mak_einlesen    = 1;
%let mak_kgs5_tag    = 1;
%let mak_kgs2_tag    = 1;
%let mak_kgs0_tag    = 1;
%let mak_export      = 1;

/************************************************************************************************************************************************/
%macro einlesen();
/* 
   Fallzahlen Zeitreihe: 
   
   Quelle: rki_covid.csv von https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0/explore
   (Satzbeschreibung: https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0)
*/
  filename imp "&mypath.&in_rki_covid19_zr." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..rki_covid19
    replace;
    delimiter = ',';
    getnames = yes;
  run;

  data &lib_work..rki_covid19;
    set &lib_work..rki_covid19;
    format upd_dat ddmmyyp10.;
    jj = input(substr(datenstand,7,4),4.);
    mm = input(substr(datenstand,4,2),2.);
    tt = input(substr(datenstand,1,2),2.);
    upd_dat = mdy(mm,tt,jj);
    drop mm tt jj datenstand;
  run;
  data &lib_work..rki_covid19;
    set &lib_work..rki_covid19;
    rename upd_dat = datenstand;
  run;

  data &lib_ziel..rki_covid19;
    set &lib_work..rki_covid19;
  run;

/*----------------------------------------------------------------------------------------------------------------------------------*/
/* 
   Fallzahlen aktueller Stand: 
   
   Die Datei mit den aktuellen Fallzahlen enthält die Einwohnerzahlen, die für die Berechnung der Inzidenzen benötigt wird. 
   Zusätzlich wird aus dieser Datei die Landkreisbezeichnung übernommen.

   Quelle: RKI_Corona_Landkreise.csv von https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0/explore?location=51.159939%2C10.714458%2C6.71&showTable=true
   (Satzbeschreibung: https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_Landkreisdaten/FeatureServer/0)
*/
  filename imp "&mypath.&in_rki_covid19_akt." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..rki_covid19_kgs_ist
    replace;
    delimiter = ',';
    getnames = yes;
  run;

  data &lib_work..rki_covid19_kgs_ist;
    set &lib_work..rki_covid19_kgs_ist;
    format upd_dat ddmmyyp10.;
    jj = input(substr(last_update,7,4),4.);
    mm = input(substr(last_update,4,2),2.);
    tt = input(substr(last_update,1,2),2.);
    upd_dat = mdy(mm,tt,jj);
    drop mm tt jj last_update;
  run;
  data &lib_work..rki_covid19_kgs_ist;
    set &lib_work..rki_covid19_kgs_ist;
    rename upd_dat = datenstand;
  run;

  data &lib_ziel..rki_covid19_kgs_ist;
    set &lib_work..rki_covid19_kgs_ist;
  run;
  
/*----------------------------------------------------------------------------------------------------------------------------------*/
/* 
   Impfquoten: 
   
   Quelle: https://github.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland/blob/master/Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv
   (weitere Hinweise: https://github.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland)
*/
  filename imp "&mypath.&in_rki_impfzahlen." encoding='utf-8';
/*
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..rki_impfquoten
    replace;
    delimiter = ',';
    getnames = yes;
  run;
*/
/* läuft auf Fehler, da Landkreis-ID als Zahl interpretiert wird und es auch die Ausprägung "u" gibt, daher Vorgabe Satzbeschreibung nötig */

  data &lib_work..rki_impfquoten;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */

    infile IMP 
      delimiter = ',' 
      MISSOVER 
      DSD 
      lrecl=13106 
      firstobs=2 
      ;
    informat 
      Impfdatum yymmdd10.
      LandkreisId_Impfort $5.
      Altersgruppe $5.
      Impfschutz best32.
      Anzahl best32. 
      ;
    format 
      Impfdatum ddmmyyp10.
      LandkreisId_Impfort $5.
      Altersgruppe $5. 
      Impfschutz best12.
      Anzahl best12. 
      ;
    input
      Impfdatum
      LandkreisId_Impfort $
      Altersgruppe $
      Impfschutz
      Anzahl
      ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
  run;

  proc sql noprint nowarnrecurs;
    select
      max(impfdatum)
     into
      :m_dat
     from
      &lib_work..rki_impfquoten
    ;  
  quit;

  data &lib_ziel..rki_impfquoten;
    set &lib_work..rki_impfquoten;
    format datenstand ddmmyyp10.;
    datenstand = &m_dat.;
  run;

/*----------------------------------------------------------------------------------------------------------------------------------*/
/* 
   Mobilitaet: 
   
   Quelle: https://github.com/rocs-org/covid-mobility-data/blob/main/data/mobility_change/mobility_change_kreise.csv
   (weitere Hinweise: https://github.com/rocs-org/covid-mobility-data)
*/
  filename imp "&mypath.&in_mobilitaet_lkr." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..rki_mobilitaet
    replace;
    delimiter = ',';
    getnames = yes;
  run;

/*
Bei der Mobilität wird abweichend vom RKI die NUTS-Systematik der Europäischen Union verwendet.

Wikipedia per 18.12.2021:
"NUTS bezeichnet eine hierarchische Systematik zur eindeutigen Identifizierung und Klassifizierung der räumlichen Bezugseinheiten der 
 amtlichen Statistik in den Mitgliedstaaten der Europäischen Union. Sie lehnt sich eng an die Verwaltungsgliederung der einzelnen Länder 
 an. In der Regel entspricht eine NUTS-Ebene einer Verwaltungsebene oder einer räumlichen Aggregation von Verwaltungseinheiten. Eine 
 vergleichbare Systematik gibt es auch in den EFTA- und CEC-Ländern.
 Diese Systematik wurde 1980 vom Europäischen Amt für Statistik in Luxemburg entwickelt, um regionale Raumeinheiten innerhalb Europas 
 auch international statistisch vergleichen zu können. NUTS-Regionen sind die Grundlage für die quantitative Beurteilung von Regionen 
 durch die EU. Im Rahmen der Regionalpolitik werden Fördermittel konkreten NUTS-Regionen (vor allem NUTS-3-Regionen) zugewiesen."

Um die Mobiltätszahlen an den Dataframe anspielen zu können, müssen die NUTS-Werte in dien amtlichen Gemeindeschlüssel (KGS) überführt werden.

Eine offizielle Übersicht zur Zuordnung einzelner PLZ zum NUTS3 (pc2020_DE_NUTS-2021_v2.0.zip: gleichnamige CSV) ist unter dem 
Link https://gisco-services.ec.europa.eu/tercet/flat-files zu finden.

Eine kostenfreie Zuordnung der PLZ zum KGS (zuordnung_plz_ort_landkreis.csv) ist auf der Seite https://www.suche-postleitzahl.org/downloads zu finden. 
Die Aktualität hier muss nicht immer gegeben sein.

Aus den GDV-Rundschreiben zu Regionalisierung in Kraftfahrt ist bekannt, dass es PLZ gibt, die zu mehreren Landkreisen (bzw. Bundesländern) gehören. 
Damit muss man bei Verwendung beider Tabellen für das Mapping Zuordnungen bei den nichteindeutigen Postleitzahlen treffen.

Nach der Satzbeschreibung zu den aktuellen Fallzahlen (RKI_Corona_Landkreise.csv) ist jedoch eine Zuordnung KGS - NUTS3 inklusive der 
notwendigen Festlegung schon vorhanden. Auf diese wird im Folgenden zugegriffen.
*/

  filename imp "&mypath.&in_schl_nuts." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..schl_zuord_plz_nuts3
    replace;
    delimiter = ';';
    getnames = yes;
  run;

  filename imp "&mypath.&in_schl_plz_kgs." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..schl_plz_kgs
    replace;
    delimiter = ',';
    getnames = yes;
  run;

/* Aufbau Übersetzungstabelle NUTS3 <-> KGS */
  proc sql noprint nowarnrecurs;
    create table &lib_work..temp_zuord as 
      select
        nuts as nuts3,
        rs as kgs,
        count(*) as anz
      from
        &lib_work..rki_covid19_kgs_ist
      group by 1,2
    ;
    create table &lib_work..temp as  /* Sonderschleife für Berlin */
      select distinct
        t1.NUTS3,
        floor(t2.ags/1000) as kgs
      from
        &lib_work..schl_zuord_plz_nuts3 as t1 inner join
        &lib_work..schl_plz_kgs as t2
        on input(t1.code,5.) = t2.plz and
           floor(t2.ags/1000) = 11000
    ;
    insert into &lib_work..temp_zuord
      select 
        *, 1
      from
        &lib_work..temp
    ;
    update &lib_work..temp_zuord as t1
      set nuts3 = (select nuts3 from temp as t2 where kgs = 11000)
      where floor(kgs/1000) = 11
    ;
  quit;
  
/* Prüfung, ob Übersetzungstabelle vollständig */
  proc sql noprint nowarnrecurs;
    create table &lib_work..temp as
      select distinct
        nuts3
      from
        &lib_work..rki_mobilitaet
      where
        not(nuts3 in (select distinct nuts3 from &lib_work..temp_zuord))
    ;
    create table &lib_work..temp as
      select distinct
        t1.nuts3,
        floor(t3.ags/1000) as kgs
      from
        (&lib_work..temp as t1 inner join
         &lib_work..schl_zuord_plz_nuts3 as t2
         on t1.nuts3 = t2.nuts3) inner join
        &lib_work..schl_plz_kgs as t3
        on input(t2.code,5.) = t3.plz
    ;
    insert into &lib_work..temp_zuord
      select 
        *, 1
      from
        &lib_work..temp
    ;    
  quit;
  
  proc sql noprint nowarnrecurs;
    create table &lib_work..rki_mobilitaet as
      select 
        t1.*,
        t2.kgs
      from
        &lib_work..rki_mobilitaet as t1 left join
        &lib_work..temp_zuord as t2 
        on t1.nuts3 = t2.nuts3
    ;
  quit;

  proc sql noprint nowarnrecurs;
    select
      max(date)
     into
      :m_dat
     from
      &lib_work..rki_mobilitaet
    ;  
  quit;

  data &lib_ziel..rki_mobilitaet;
    set &lib_work..rki_mobilitaet;
    format datenstand ddmmyyp10.;
    datenstand = &m_dat.;
  run; 

/* Mobilitaet nach Bundesland */
/* Quelle: https://github.com/rocs-org/covid-mobility-data/blob/main/data/mobility_change/mobility_change_bundeslaender.csv   */
  filename imp "&mypath.&in_mobilitaet_bl." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..rki_mobilitaet_bl
    replace;
    delimiter = ',';
    getnames = yes;
  run;

/* Mobilitaet nach Gesamt-Deutschland */ 
/* Quelle: https://github.com/rocs-org/covid-mobility-data/blob/main/data/mobility_change/mobility_change_germany.csv  */
  filename imp "&mypath.&in_mobilitaet_ges." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..rki_mobilitaet_ges
    replace;
    delimiter = ',';
    getnames = yes;
  run;

/*----------------------------------------------------------------------------------------------------------------------------------*/
/* 
   DIVI-Zahlen: 
   
   Quelle: Landkreis-Daten aus https://www.intensivregister.de/#/aktuelle-lage/downloads
  (weitere Hinweise: https://edoc.rki.de/bitstream/handle/176904/7989/Tagesdaten%20CSV%20Erkl%c3%a4rung%20Stand%2029.3.pdf?sequence=1&isAllowed=y)
*/
  filename imp "&mypath.&in_divi." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..rki_divi
    replace;
    delimiter = ',';
    getnames = yes;
  run;

  proc sql noprint nowarnrecurs;
    select
      max(date)
     into
      :m_dat
     from
      &lib_work..rki_divi
    ;  
  quit;

  data &lib_ziel..rki_divi;
    set &lib_work..rki_divi;
    format datenstand ddmmyyp10.;
    datenstand = &m_dat.;
  run; 

/*----------------------------------------------------------------------------------------------------------------------------------*/
/* 
   Hospitalisierung-Zahlen: 
   
   Quelle: Aktuell_Deutschland_COVID-19-Hospitalisierungen.csv
   aus https://github.com/robert-koch-institut/COVID-19-Hospitalisierungen_in_Deutschland
*/
  filename imp "&mypath.&in_hospital." encoding='utf-8';
  proc import 
    datafile = imp
    dbms     = tab
    out      = &lib_work..rki_hospital
    replace;
    delimiter = ',';
    getnames = yes;
  run;

  proc sql noprint nowarnrecurs;
    select
      max(datum)
     into
      :m_dat
     from
      &lib_work..rki_hospital
    ;  
  quit;

  data &lib_ziel..rki_hospital;
    set &lib_work..rki_hospital;
    format datenstand ddmmyyp10.;
    datenstand = &m_dat.;
  run; 
%mend einlesen;

/************************************************************************************************************************************************/
%macro berechnungen(tabelle,ebene,m_impfschutz);
/* Berechnung kumulierter Werte */
  proc sort data = &tabelle.;
    by &ebene. melde_dat;
  run;

  data &tabelle.;
    set &tabelle.;
    by &ebene.;
    if first.&ebene. then anz_fall_kum = 0;
       anz_fall_kum + anz_fall_neu;
    if first.&ebene. then anz_tote_kum = 0;
       anz_tote_kum + anz_tote_neu;
    %do i = 1 %to &m_impfschutz.;
        if first.&ebene. then anz_impfung_&i._kum = 0;
           anz_impfung_&i._kum + anz_impfung_&i.;
    %end;
  run;

  proc contents
    data = &tabelle.
    out  = &lib_work..var_lst
    short
    noprint
    ;
  run;
  proc sql noprint nowarnrecurs;
    select
      varnum
     into
      :group_lst separated by ','
     from
      &lib_work..var_lst
     order by varnum
    ;
    select
      max(varnum)
     into
      :max_num
     from
      &lib_work..var_lst
    ;
  quit;

/*  Im Folgenden werden folgende Kennzahlen berechnet:
    - 7-Tage-Inzidenz
    - R-Wert 4-Tage
    - R-Wert 7-Tage
    Alle Kennzahlen hatten im Laufe der Pandemie Auswirkungen auf die Maßnahmen von Bund/Land/Landkreis. 
    Der Einfluss dieser Kennzahlen änderte sich im Laufe der Zeit.

    7-Tage-Inzidenz:
    Bei der 7-Tage-Inzidenz liegt das Meldedatum beim Gesundheitsamt zugrunde, also das Datum, an dem das lokale Gesundheitsamt 
    Kenntnis über den Fall erlangt und ihn elektronisch erfasst hat. Die Kennzahl beinhaltet die Anzahl der neu gemeldeten Fälle 
    der jeweils letzten 7 Tage je 100.000 Einwohner.

    R-Wert:
    Die Berechnung des R-Wertes folgt dem Paper https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Projekte_RKI/R-Beispielrechnung.xlsx?__blob=publicationFile.

    Damit gilt:
    R-Wert 4-Tage: Anzahl neuer Fälle Meldedatum + 3 Vortage / Anzahl neuer Fälle aus dem Zeitraum 4. - 7. Vortag
    R-Wert 7-Tage: Anzahl neuer Fälle aus Zeitraum Folgetag - 5.Vortag / Anzahl neuer Fälle aus Zeitraum 3. - 9. Vortag
  
    7-Tage-Inzidenz = Meldedatum - 6 bis Meldedatum
    R-Wert-4 Nenner = Meldedatum - 3 bis Meldedatum + 0, Zähler = Meldedatum - 7 bis Meldedatum - 4
    R-Wert-7 Nenner = Meldedatum - 5 bis Meldedatum + 1, Zähler = Meldedatum - 9 bis Meldedatum - 3
*/
  %let lst_anz_tage = 7 4 4 7 7;
  %let lst_tag_start = 6 3 7 5 9;
  %let lst_name = anz_fall_7 oben_4 unten_4 oben_7 unten_7;
  
  %do i = 1 %to %sysfunc(countw(&lst_anz_tage.,' '));
      proc sql noprint nowarnrecurs;
        create table &tabelle. as
          select
            t1.*,
            sum(coalesce(t2.anz_fall_neu,0)) as %scan(&lst_name.,&i.,' ')
          from
            &tabelle. as t1 left join
            &tabelle. as t2
            on t1.&ebene. = t2.&ebene. and
               t2.melde_dat between t1.melde_dat - %scan(&lst_tag_start.,&i.,' ') and 
                                    t1.melde_dat - (%scan(&lst_tag_start.,&i.,' ') - %scan(&lst_anz_tage.,&i.,' ') + 1)
          group by &group_lst.
        ;
      quit;
      %let group_lst = &group_lst., %eval(&max_num. + &i.);    
  %end;
  
  data &tabelle.;
    set &tabelle.;
    inzidenz = round(coalesce(anz_fall_7,0) / einwohner * 100000,0.1);
    if missing(unten_4) or unten_4 = 0 then r_wert_4 = 0; 
       else r_wert_4 = round(coalesce(oben_4,0)/coalesce(unten_4,1),0.01);
    if missing(unten_7) or unten_7 = 0 then r_wert_7 = 0;
       else r_wert_7 = round(coalesce(oben_7,1)/coalesce(unten_7,1),0.01);
    drop &lst_name.;
  run;

/* R-Werte, die aufgrund Definition nicht existieren können werden auf NULL gesetzt */
  proc sql noprint nowarnrecurs;
    create table &lib_work..temp as
      select
        &ebene.,
        min(melde_dat) as min_dat
      from
        &tabelle.
      where
        anz_fall_neu > 0
      group by 1
    ;
    create table &tabelle. as
      select distinct
        t1.*,
        t2.min_dat
      from
        &tabelle. as t1 left join
        &lib_work..temp as t2
        on t1.&ebene. = t2.&ebene.
    ; 
    update &tabelle.
      set r_wert_4 =.
      where 
        melde_dat <= min_dat + 7 and
        not(missing(r_wert_4))
    ;
    update &tabelle.
      set r_wert_7 =.
      where 
        melde_dat <= min_dat + 9 and
        not(missing(r_wert_7))
    ;
    alter table &tabelle.
      drop min_dat
    ;
  quit;

  proc sort data = &tabelle.;
    by &ebene. melde_dat;
  run;

  data &tabelle.;
    set &tabelle.;
    format infektionsrate letalitaetsrate percentn9.2;
    infektionsrate = round(anz_fall_kum / einwohner,0.0001);
    if anz_fall_kum = 0 then letalitaetsrate = 0; else letalitaetsrate = round(anz_tote_kum / anz_fall_kum,0.0001);
    if missing(letalitaetsrate) then letalitaetsrate = 0;
    %do i = 1 %to &m_impfschutz.;
        quote_impfung_&i. = round(anz_impfung_&i._kum / einwohner,0.0001);
    %end;
  run;
%mend berechnungen;

/************************************************************************************************************************************************/
%macro kgs5_tag();
/* Aufbereitung je Kreis und Tag */
/* vollständige Datumsliste */
  proc sql noprint nowarnrecurs;
    select
      max(meldedatum)
     into
      :dat_bis
     from
      &lib_work..rki_covid19
    ;
  quit;

  data _null_;
    call symput('dat_ab',mdy(1,1,2020));
  run;

  data &lib_work..temp_meldedat;
    format melde_dat ddmmyyp10.;
    melde_dat = &dat_ab.;
    do while (melde_dat <= &dat_bis.);
       output;
       melde_dat = melde_dat + 1;
    end;
  run;

/* vollständige KGS-Liste */
  proc sql noprint nowarnrecurs;
    create table &lib_work..temp_kgs as
      select distinct
        cats(IdLandkreis) as kgs5
      from
        &lib_work..rki_covid19
    ;
  quit;

  proc sql noprint nowarnrecurs;
    create table &lib_work..temp_analyse as /* Kreuzprodukt */
      select
        melde_dat,
        kgs5,
        0 as anz_fall_neu,
        0 as anz_tote_neu
      from
        &lib_work..temp_meldedat,
        &lib_work..temp_kgs
    ;
    insert into &lib_work..temp_analyse
      select
        Meldedatum,
        cats(IdLandkreis),
        sum(anzahlfall),
        sum(anzahltodesfall)
      from
        &lib_work..rki_covid19
      where
        meldedatum <= &dat_bis.+1
      group by 1,2
    ;
    create table &lib_work..temp_analyse as
      select 
        t1.melde_dat,
        put(input(t1.kgs5,5.),z5.) as kgs5,
        t2.gen as kgs_name, 
        t2.bez as kgs_art,
        t2.ewz as einwohner,
        sum(anz_fall_neu) as anz_fall_neu,
        sum(anz_tote_neu) as anz_tote_neu
      from
        &lib_work..temp_analyse as t1 left join
        &lib_work..rki_covid19_kgs_ist as t2
        on input(t1.kgs5,6.) = t2.rs
      group by 1,2,3,4,5 
    ;
  quit;  

/* Erweiterung um Impfzahlen */
  proc sql noprint nowarnrecurs;  
    select
      max(impfschutz)
     into
      :m_impfschutz 
     from
      &lib_work..rki_impfquoten
    ;
    create table &lib_work..temp as
      select
        impfdatum,
        landkreisid_impfort
        %do i = 1 %to &m_impfschutz.;
            %str(,)sum(case when impfschutz = &i. then anzahl else 0 end) as anz_impfung_&i.
        %end;
      from
        &lib_work..rki_impfquoten
      group by 1,2
    ;
  quit;  

  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse as
      select
        t1.*
        %do i = 1 %to &m_impfschutz.;
            %str(,)coalesce(t2.anz_impfung_&i.,0) as anz_impfung_&i.
        %end;
      from
        &lib_work..temp_analyse as t1 left join
        &lib_work..temp as t2
        on t1.kgs5 = t2.landkreisid_impfort and
           t1.melde_dat = t2.impfdatum
    ;
  quit;

 /* 
   Sonderschleife für Berlin: Beim Impfen nur als Gesamt, bei den Fallzahlen getrennt nach Stadtbezirk
   Aus diesem Grund werden die vorhandenen Impfquoten für die gesamte Stadt als identisch angenommen und 
   die Impfzahlen entsprechend der jeweiligen Einwohnerzahlen auf die einzelnen Stadtbezirke aufgeteilt.
   Differenz aus berechneten Werten und dem tatsächlichen Gesamtwert wird im Stadtbezirk mit der höchsten ID bereinigt
 */
  proc sql noprint nowarnrecurs;
    create table &lib_work..temp_berlin as
      select distinct
        melde_dat,
        kgs5,
        einwohner
      from
        &lib_work..temp_analyse
      where
        kgs5 like '11%'
    ;
    create table &lib_work..temp_berlin as
      select
        t1.*
        %do i = 1 %to &m_impfschutz.;
            %str(,)coalesce(t2.anz_impfung_&i.,0) as anz_impfung_&i._g
        %end;
      from
        &lib_work..temp_berlin as t1 left join
        &lib_work..temp as t2
        on t1.melde_dat = t2.Impfdatum and
           t2.LandkreisId_Impfort = '11000'
    ;
    select
      sum(einwohner),
      max(kgs5)
     into
      :ew_ges,
      :m_kgs5
     from
      &lib_work..temp_berlin
     where
      melde_dat = mdy(1,1,2020)
    ;
    create table &lib_work..temp_berlin as
      select
        *
        %do i = 1 %to &m_impfschutz.;
            %str(,)round(anz_impfung_&i._g * einwohner / &ew_ges.,1) as anz_impfung_&i.
        %end;
      from
        &lib_work..temp_berlin
    ;
    create table &lib_work..temp1 as
      select
        melde_dat
        %do i = 1 %to &m_impfschutz.;
            %str(,)max(anz_impfung_&i._g) as anz_impfung_&i._g
            %str(,)sum(anz_impfung_&i.) as anz_impfung_&i.
        %end;
      from
        &lib_work..temp_berlin
      group by 1
    ;
    create table &lib_work..temp1 as
      select
        *
        %do i = 1 %to &m_impfschutz.;
            %str(,)anz_impfung_&i._g - anz_impfung_&i. as diff_&i.
        %end;
      from 
        &lib_work..temp1
    ;
    create table &lib_work..temp_berlin as
      select 
        t1.*
        %do i = 1 %to &m_impfschutz.;
            %str(,)t2.diff_&i.
        %end;
      from
        &lib_work..temp_berlin as t1 left join
        &lib_work..temp1 as t2
        on t1.melde_dat = t2.melde_dat
    ;
    update &lib_work..temp_berlin
      set 
        kgs5 = "&m_kgs5."
        %do i = 1 %to &m_impfschutz.;
            %str(,)anz_impfung_&i. = anz_impfung_&i. + diff_&i.
        %end;
      where
        kgs5 = "&m_kgs5."
    ;
    create table &lib_work..temp_analyse as
      select
        t1.*
        %do i = 1 %to &m_impfschutz.;
            %str(,)coalesce(t2.anz_impfung_&i.,0) as anz_impfung_&i._b
        %end;
      from
        &lib_work..temp_analyse as t1 left join
        &lib_work..temp_berlin as t2
        on t1.kgs5 = t2.kgs5 and
           t1.melde_dat = t2.melde_dat
    ;
    update &lib_work..temp_analyse
      set 
        %do i = 1 %to &m_impfschutz.;
            %if &i. = 1 %then anz_impfung_&i. = anz_impfung_&i. + anz_impfung_&i._b;
                %else %str(,)anz_impfung_&i. = anz_impfung_&i. + anz_impfung_&i._b;
        %end;
    ;
    alter table &lib_work..temp_analyse
      drop 
        %do i = 1 %to &m_impfschutz.;
            %if &i. = 1 %then anz_impfung_&i._b; %else %str(,)anz_impfung_&i._b;
        %end;
    ;
  quit;
  
/* Zusammenfassung der Zahlen auch noch für Gesamtberlin */  
  proc sql noprint nowarnrecurs;
    create table &lib_work..temp2 as
      select
        melde_dat,
        '11000' as kgs5,
        'Berlin' as kgs_name,
        'Kreisfreie Stadt' as kgs_art,
        sum(einwohner) as einwohner,
        sum(anz_fall_neu) as anz_fall_neu,
        sum(anz_tote_neu) as anz_tote_neu
        %do i = 1 %to &m_impfschutz.;
            %str(,)sum(anz_impfung_&i.) as anz_impfung_&i.
        %end;
      from
        &lib_work..temp_analyse
      where
        kgs5 like '11%'
      group by 1,2
    ;
    insert into &lib_work..temp_analyse
      select * from &lib_work..temp2
    ;
  quit;
 
  %berechnungen(&lib_work..temp_analyse,kgs5,&m_impfschutz.); /* Kennzahlen und Summen */

/* Erweiterung um Mobilität */
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse as
      select
        t1.*,
        coalesce(t2.mobility_change,0) as mobility_change,
        coalesce(t2.mobility_change_7day_average,0) as mobility_change_7day_average,
        coalesce(t2.mobility_change_weekly,0) as mobility_change_weekly
      from
        &lib_work..temp_analyse as t1 left join
        &lib_work..rki_mobilitaet as t2
        on input(t1.kgs5,5.) = t2.kgs and
           t1.melde_dat = t2.date
    ;
  quit;

/* Erweiterung um DIVI (Landkreis + Bundesland) */
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse as
      select
        t1.*,
        coalesce(round(t2.faelle_covid_aktuell / (t2.betten_frei + t2.betten_belegt),4),0) as intensivbetten,
        coalesce(t2.faelle_covid_aktuell,0) as faelle_covid_aktuell,
        coalesce(t2.faelle_covid_aktuell_invasiv_bea,0) as invasiv_beatmet
      from
        &lib_work..temp_analyse as t1 left join
        &lib_work..rki_divi as t2
        on input(t1.kgs5,5.) = t2.gemeindeschluessel and
           t1.melde_dat = t2.date
    ;
    create table &lib_work..temp_analyse as
      select
        t1.*,
        coalesce(round(t2.faelle_covid_aktuell / (t2.betten_frei + t2.betten_belegt),4),0) as intensivbetten_bdl,
        coalesce(t2.faelle_covid_aktuell,0) as faelle_covid_aktuell_bdl,
        coalesce(t2.invasiv_beatmet,0) as invasiv_beatmet_bdl
      from
        &lib_work..temp_analyse as t1 left join
        (select  floor(gemeindeschluessel/1000) as bdl,
                 date, 
                 sum(faelle_covid_aktuell) as faelle_covid_aktuell,
                 sum(betten_frei) as betten_frei,
                 sum(betten_belegt) as betten_belegt,
                 sum(faelle_covid_aktuell_invasiv_bea) as invasiv_beatmet
           from  &lib_work..rki_divi 
           group by 1,2) as t2
        on floor(input(t1.kgs5,5.)/1000) = t2.bdl and
           t1.melde_dat = t2.date
    ;
  quit;
 
/* Erweiterung um Hospitalisierung */
  proc sql noprint nowarnrecurs;
    create table &lib_work..temp_ew as
      select
        bl_id,
        sum(ewz) as einwohner
      from
        &lib_work..rki_covid19_kgs_ist
      group by 1
    ;
    insert into &lib_work..temp_ew
      select
        0 as bl_id,
        sum(ewz) as einwohner
      from
        &lib_work..rki_covid19_kgs_ist
      group by 1
    ;
  quit;
  
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse as
      select
        t1.*,
        round(coalesce(t2.faelle,0)/t3.einwohner*100000,0.0001) as hospitalisierung_inzidenz
      from
        (&lib_work..temp_analyse as t1 left join
         (select  datum,
                  bundesland_id,
                  sum('7T_Hospitalisierung_Faelle'n) as faelle
            from  &lib_work..rki_hospital
            group by 1,2) as t2 
         on floor(input(t1.kgs5,5.)/1000) = t2.bundesland_id and
            t1.melde_dat = t2.datum) left join
        &lib_work..temp_ew as t3
        on floor(input(t1.kgs5,5.)/1000) = t3.bl_id
    ;
  quit;

  proc sql noprint nowarnrecurs inobs = 1;
    select
      'mdy('||cats(month(datenstand))||','||cats(day(datenstand))||','||cats(year(datenstand))||')'
     into
      :stand
     from
      &lib_work..rki_covid19_kgs_ist 
    ;  
  quit;
  proc sql noprint nowarnrecurs;
    select
      'mdy('||cats(month(max(impfdatum)))||','||cats(day(max(impfdatum)))||','||cats(year(max(impfdatum)))||')'
     into
      :stand_i
     from
      &lib_work..rki_impfquoten
    ;  
  quit;
  proc sql noprint nowarnrecurs;
    select
      'mdy('||cats(month(max(date)))||','||cats(day(max(date)))||','||cats(year(max(date)))||')'
     into
      :stand_m
     from
      &lib_work..rki_mobilitaet
    ;  
  quit;

  data &lib_ziel..&out_landkreis.;
    set &lib_work..temp_analyse;
    format rki_stand rki_impf_stand rki_mobilitaet_stand einwohner_stand ddmmyyp10. ;
    rki_stand = &dat_bis.;
    rki_impf_stand = &stand_i.;
    rki_mobilitaet_stand = &stand_m.;
    einwohner_stand = &stand.;
  run;
%mend kgs5_tag;

/************************************************************************************************************************************************/
/* Aufbereitung je Bundesland und Tag */
%macro kgs2_tag();
  proc sql noprint nowarnrecurs;  
    select
      max(impfschutz)
     into
      :m_impfschutz 
     from
      &lib_work..rki_impfquoten
    ;
    create table &lib_work..temp_analyse_bdl as
      select
        t1.melde_dat,
        substr(t1.kgs5,1,2) as kgs2,
        t2.bl as bdl_name,
        sum(t1.einwohner) as einwohner,
        sum(t1.anz_fall_neu) as anz_fall_neu,
        sum(t1.anz_tote_neu) as anz_tote_neu
        %do i = 1 %to &m_impfschutz.;
            %str(,)sum(t1.anz_impfung_&i.) as anz_impfung_&i.
        %end;
      from
        &lib_work..temp_analyse as t1 left join
        (select distinct bl_id, bl from &lib_work..rki_covid19_kgs_ist) as t2
        on input(substr(t1.kgs5,1,2),2.) = t2.bl_id
      where
        not (t1.kgs5 = '11000')  /* Berlin als Stadtbezirke und als Gesamt in ...temp_analyse */
      group by 1,2,3
    ;
  quit;  

  %berechnungen(&lib_work..temp_analyse_bdl,kgs2,&m_impfschutz.);

/* Erweiterung um Mobilität */
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse_bdl as
      select
        t1.*,
        coalesce(t2.mobility_change,0) as mobility_change,
        coalesce(t2.mobility_change_7day_average,0) as mobility_change_7day_average,
        coalesce(t2.mobility_change_weekly,0) as mobility_change_weekly
      from
        &lib_work..temp_analyse_bdl as t1 left join
        &lib_work..rki_mobilitaet_bl as t2
        on t1.bdl_name = t2.name and
           t1.melde_dat = t2.date
    ;
  quit;

/* Erweiterung um DIVI (Bundesland) */
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse_bdl as
      select
        t1.*,
        coalesce(round(t2.faelle_covid_aktuell / (t2.betten_frei + t2.betten_belegt),4),0) as intensivbetten,
        coalesce(t2.faelle_covid_aktuell,0) as faelle_covid_aktuell,
        coalesce(t2.invasiv_beatmet,0) as invasiv_beatmet
      from
        &lib_work..temp_analyse_bdl as t1 left join
        (select  floor(gemeindeschluessel/1000) as bdl,
                 date, 
                 sum(faelle_covid_aktuell) as faelle_covid_aktuell,
                 sum(betten_frei) as betten_frei,
                 sum(betten_belegt) as betten_belegt,
                 sum(faelle_covid_aktuell_invasiv_bea) as invasiv_beatmet
           from  &lib_work..rki_divi 
           group by 1,2) as t2
        on input(t1.kgs2,2.) = t2.bdl and
           t1.melde_dat = t2.date
    ;
  quit;
 
/* Erweiterung um Hospitalisierung */ 
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse_bdl as
      select
        t1.*,
        round(coalesce(t2.faelle,0)/t1.einwohner*100000,0.0001) as hospitalisierung_inzidenz
      from
        &lib_work..temp_analyse_bdl as t1 left join
        (select  datum,
                 bundesland_id,
                 sum('7T_Hospitalisierung_Faelle'n) as faelle
           from  &lib_work..rki_hospital
           group by 1,2) as t2 
        on input(t1.kgs2,2.) = t2.bundesland_id and
           t1.melde_dat = t2.datum
    ;
  quit;

  proc sql noprint nowarnrecurs inobs = 1;
    select
      'mdy('||cats(month(einwohner_stand))||','||cats(day(einwohner_stand))||','||cats(year(einwohner_stand))||')',
      'mdy('||cats(month(rki_stand))||','||cats(day(rki_stand))||','||cats(year(rki_stand))||')',
      'mdy('||cats(month(rki_impf_stand))||','||cats(day(rki_impf_stand))||','||cats(year(rki_impf_stand))||')',
      'mdy('||cats(month(rki_mobilitaet_stand))||','||cats(day(rki_mobilitaet_stand))||','||cats(year(rki_mobilitaet_stand))||')'
     into
      :stand_e,
      :stand_r,
      :stand_i,
      :stand_m
     from
      &lib_ziel..&out_landkreis.
    ;  
  quit;

  data &lib_ziel..&out_bundesland.;
    set &lib_work..temp_analyse_bdl;
    format rki_stand rki_impf_stand rki_mobilitaet_stand einwohner_stand ddmmyyp10.;
    rki_stand = &stand_r.;
    rki_impf_stand = &stand_i.;
    rki_mobilitaet_stand = &stand_m.;
    einwohner_stand = &stand_e.;
  run;
%mend kgs2_tag;

/************************************************************************************************************************************************/
/* Aufbereitung Gesamt-Deutschland je Tag */
%macro kgs0_tag();
  proc sql noprint nowarnrecurs;  
    select
      max(impfschutz)
     into
      :m_impfschutz 
     from
      &lib_work..rki_impfquoten
    ;
    create table &lib_work..temp_analyse_ges as
      select
        melde_dat,
        '00' as kgs,
        sum(einwohner) as einwohner,
        sum(anz_fall_neu) as anz_fall_neu,
        sum(anz_tote_neu) as anz_tote_neu
        %do i = 1 %to &m_impfschutz.;
            %str(,)sum(anz_impfung_&i.) as anz_impfung_&i.
        %end;
      from
        &lib_work..temp_analyse
      where
        not (kgs5 = '11000')  /* Berlin als Stadtbezirke und als Gesamt in ...temp_analyse */
      group by 1,2
    ;
/* Ergänzung der Impfzahlen mit unbekanntem Landkreis für Deutschland gesamt */
    create table &lib_work..temp as
      select
        '00' as kgs,
        impfdatum
        %do i = 1 %to &m_impfschutz.;
            %str(,)sum(anzahl) as anz_impfung_&i.
        %end;
      from
        &lib_work..rki_impfquoten
      where
        strip(landkreisid_impfort) = 'u'
      group by 1,2
    ;
    create table &lib_work..temp_analyse_ges as
      select
        t1.*
        %do i = 1 %to &m_impfschutz.;
            %str(,)coalesce(t2.anz_impfung_&i.,0) as anz_impfung_&i._u
        %end;
      from
        &lib_work..temp_analyse_ges as t1 left join
        &lib_work..temp as t2
        on t1.melde_dat = t2.impfdatum
    ;
    update &lib_work..temp_analyse_ges /* Aktualisierung nur in den Datensätzen, bei denen ein Wert für "unbekannt" vorliegt */
      set
        %do i = 1 %to &m_impfschutz.;
            %if &i. < &m_impfschutz. %then anz_impfung_&i. = anz_impfung_&i. + anz_impfung_&i._u%str(,);
                %else anz_impfung_&i. = anz_impfung_&i. + anz_impfung_&i._u;
        %end;
      where
        %do i = 1 %to &m_impfschutz.;
            %if &i. < &m_impfschutz. %then anz_impfung_&i._u + ;
                %else anz_impfung_&i._u > 0;
        %end;
    ;
    alter table &lib_work..temp_analyse_ges
      drop
        %do i = 1 %to &m_impfschutz.;
            %if &i. < &m_impfschutz. %then anz_impfung_&i._u%str(,);
                %else anz_impfung_&i._u;
        %end;
    ;
  quit;  

  %berechnungen(&lib_work..temp_analyse_ges,kgs,&m_impfschutz.);

/* Erweiterung um Mobilität */
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse_ges as
      select
        t1.*,
        coalesce(t2.mobility_change,0) as mobility_change,
        coalesce(t2.mobility_change_7day_average,0) as mobility_change_7day_average,
        coalesce(t2.mobility_change_weekly,0) as mobility_change_weekly
      from
        &lib_work..temp_analyse_ges as t1 left join
        &lib_work..rki_mobilitaet_ges as t2
        on t1.melde_dat = t2.date
    ;
  quit;

/* Erweiterung um DIVI (Bundesland) */
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse_ges as
      select
        t1.*,
        coalesce(round(t2.faelle_covid_aktuell / (t2.betten_frei + t2.betten_belegt),4),0) as intensivbetten,
        coalesce(t2.faelle_covid_aktuell,0) as faelle_covid_aktuell,
        coalesce(t2.invasiv_beatmet,0) as invasiv_beatmet
      from
        &lib_work..temp_analyse_ges as t1 left join
        (select  date, 
                 sum(faelle_covid_aktuell) as faelle_covid_aktuell,
                 sum(betten_frei) as betten_frei,
                 sum(betten_belegt) as betten_belegt,
                 sum(faelle_covid_aktuell_invasiv_bea) as invasiv_beatmet
           from  &lib_work..rki_divi 
           group by 1) as t2
        on t1.melde_dat = t2.date
    ;
  quit;
 
/* Erweiterung um Hospitalisierung */ 
  proc sql noprint nowarnrecurs;  
    create table &lib_work..temp_analyse_ges as
      select
        t1.*,
        round(coalesce(t2.faelle,0)/t1.einwohner*100000,0.0001) as hospitalisierung_inzidenz
      from
        &lib_work..temp_analyse_ges as t1 left join
        (select  datum,
                 bundesland_id,
                 sum('7T_Hospitalisierung_Faelle'n) as faelle
           from  &lib_work..rki_hospital
           group by 1,2) as t2 
        on t2.bundesland_id = 0 and
           t1.melde_dat = t2.datum
    ;
  quit;

  proc sql noprint nowarnrecurs inobs = 1;
    select
      'mdy('||cats(month(einwohner_stand))||','||cats(day(einwohner_stand))||','||cats(year(einwohner_stand))||')',
      'mdy('||cats(month(rki_stand))||','||cats(day(rki_stand))||','||cats(year(rki_stand))||')',
      'mdy('||cats(month(rki_impf_stand))||','||cats(day(rki_impf_stand))||','||cats(year(rki_impf_stand))||')',
      'mdy('||cats(month(rki_mobilitaet_stand))||','||cats(day(rki_mobilitaet_stand))||','||cats(year(rki_mobilitaet_stand))||')'
     into
      :stand_e,
      :stand_r,
      :stand_i,
      :stand_m
     from
      &lib_ziel..&out_landkreis.
    ;  
  quit;

  data &lib_ziel..&out_gesamt.;
    set &lib_work..temp_analyse_ges;
    format rki_stand rki_impf_stand rki_mobilitaet_stand einwohner_stand ddmmyyp10.;
    rki_stand = &stand_r.;
    rki_impf_stand = &stand_i.;
    rki_mobilitaet_stand = &stand_m.;
    einwohner_stand = &stand_e.;
  run;
%mend kgs0_tag;

/***************************************************************************************************************************/
%macro export();
  %let lst_exp = out_landkreis out_bundesland out_gesamt;
  %do i = 1 %to %sysfunc(countw(&lst_exp.));
      %let tab = %scan(&lst_exp.,&i.,' ');
      %let tab = &&&tab.;
      proc export
        data     = &lib_ziel..&tab. 
        outfile  = "&mypath.&tab..csv" 
        dbms     = csv 
        replace;
        delimiter = ';';
        putnames  = yes;
      run;
  %end;
%mend export;

/***************************************************************************************************************************/
/* Programm                                                                                                                */
%macro hauptprogramm();
  %if &mak_einlesen. = 1 %then %einlesen();
  %if &mak_kgs5_tag. = 1 %then %kgs5_tag();
  %if &mak_kgs2_tag. = 1 %then %kgs2_tag();
  %if &mak_kgs0_tag. = 1 %then %kgs0_tag();
  %if &mak_export.   = 1 %then %export();
%mend hauptprogramm;

%hauptprogramm();
