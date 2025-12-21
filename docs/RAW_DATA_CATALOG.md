# Raw Data Catalog

This document provides a comprehensive list of all raw data sources identified and tracked for the TYT Paper Trail project.

## Core Legislative & Campaign Finance Sources

### 1. Voteview (UCLA)
*   **Description:** The authoritative historical record of U.S. Congressional roll-call voting.
*   **Data Provided:** Legislator metadata (Bioguide IDs, ICPSR codes), individual vote records (yea/nay/present), and rollcall metadata (dates, descriptions, results).
*   **Download URLs:**
    *   [HSall_members.csv](https://voteview.com/static/data/out/members/HSall_members.csv): Metadata for all historical and current Members of Congress.
    *   [HSall_votes.csv](https://voteview.com/static/data/out/votes/HSall_votes.csv): Transactional records of every individual vote cast by members.
    *   [HSall_rollcalls.csv](https://voteview.com/static/data/out/rollcalls/HSall_rollcalls.csv): Metadata for rollcall votes (dates, descriptions, results).

### 2. DIME - Database on Ideology, Money in Politics, and Elections (Stanford)
*   **Description:** A massive dataset linking campaign contributions to politician ideology and behavior.
*   **Data Provided:** Transactional contribution records, contributor profiles, recipient metadata, and bill topic weights.
*   **Download URLs:**
    *   [Aggregate Recipients/Candidates (1979–2024)](https://www.dropbox.com/scl/fi/bcpmxvxwc7iwmqvchm2sp/dime_recipients_1979_2024.csv.gz?rlkey=4ii8765xw8y2svlzcbxzfvnbh&dl=1): Metadata for all recipients in the DIME v4.0 dataset.
    *   [Individual Donors/PACs (1979–2024)](https://www.dropbox.com/scl/fi/c5z45dm2g8u9ihfi7uce8/dime_contributors_1979_2024.csv.gz?rlkey=janwvetndyxe4t8tm2v5a6wbu&dl=1): Aggregated metadata for individual and committee donors.
    *   [Full SQLite Database (v4.0)](https://www.dropbox.com/scl/fi/qvopuun6m216g85azfbvg/dime_v4.sqlite3.gz?rlkey=3x7xq6vgr2onl9z30x0g8ed0h&dl=1): The complete DIME v4.0 dataset in a single relational database file.
    *   [Sparse Matrix (dime_mimp_1979_2024.rdata)](https://www.dropbox.com/scl/fi/05xajq916qj0dr0ltwcy5/dime_mimp_1979_2024.rdata?rlkey=2fq1ayztq7uy92q9mlgh5ezwv&dl=1): A high-dimensional mapping of donor-recipient relationships, optimized for R.

#### **Itemized Contribution Records (By Cycle)**

Individual gzipped CSV files containing every itemized donation for a given two-year cycle:

<details>
<summary>View Annual Download Links (1980-2024)</summary>

*   [1980 (contribDB_1980.csv.gz)](https://www.dropbox.com/scl/fi/fgk4kchmhr86e7k89fzzu/contribDB_1980.csv.gz?rlkey=9q1etiitvzzqwwomtsi7l4kfd&dl=1)
*   [1982 (contribDB_1982.csv.gz)](https://www.dropbox.com/scl/fi/26dmphuq84b8zmhelkmiq/contribDB_1982.csv.gz?rlkey=cnslknt934m0yke0c2534e42d&dl=1)
*   [1984 (contribDB_1984.csv.gz)](https://www.dropbox.com/scl/fi/zcmgqxdowr3gki1vc5lrc/contribDB_1984.csv.gz?rlkey=gga6sfzi43tigr26ungftgrxd&dl=1)
*   [1986 (contribDB_1986.csv.gz)](https://www.dropbox.com/scl/fi/wp4k9zuzm6ty6afs1x1oh/contribDB_1986.csv.gz?rlkey=tvgt92s1emtw7ok4i78l7jcxf&dl=1)
*   [1988 (contribDB_1988.csv.gz)](https://www.dropbox.com/scl/fi/oot3plqbdrrqierfx9mdz/contribDB_1988.csv.gz?rlkey=4lhyc989ss5w40i76stthmnzv&dl=1)
*   [1990 (contribDB_1990.csv.gz)](https://www.dropbox.com/scl/fi/dgq2raktk8yuccs37epb2/contribDB_1990.csv.gz?rlkey=qn38uwkteldub8gw1zkowfoym&dl=1)
*   [1992 (contribDB_1992.csv.gz)](https://www.dropbox.com/scl/fi/g8cmdl0glg2czam7orwrf/contribDB_1992.csv.gz?rlkey=9cw2e21ki1dhlqm78j5huq2pg&dl=1)
*   [1994 (contribDB_1994.csv.gz)](https://www.dropbox.com/scl/fi/xhspr7kwlt7fnn8lrcasp/contribDB_1994.csv.gz?rlkey=ev222r36r2fb24ouhbvaufsj9&dl=1)
*   [1996 (contribDB_1996.csv.gz)](https://www.dropbox.com/scl/fi/566kucvtzsojyarj5nqy3/contribDB_1996.csv.gz?rlkey=anhxz6y8ez309mngxv2r5fo2n&dl=1)
*   [1998 (contribDB_1998.csv.gz)](https://www.dropbox.com/scl/fi/b3mav3barc0dwiry5d773/contribDB_1998.csv.gz?rlkey=nhwn0dqk2apumrsj6p3crz2em&dl=1)
*   [2000 (contribDB_2000.csv.gz)](https://www.dropbox.com/scl/fi/98mnrmknv4cd5a7tjxa2p/contribDB_2000.csv.gz?rlkey=qntafhlgk9sq1lu20u3ners5z&dl=1)
*   [2002 (contribDB_2002.csv.gz)](https://www.dropbox.com/scl/fi/7gnjh8vb32s1eq9hebcln/contribDB_2002.csv.gz?rlkey=ojw6jjycw6mjhwq7r5hslwd0u&dl=1)
*   [2004 (contribDB_2004.csv.gz)](https://www.dropbox.com/scl/fi/746km8razol0c8u65l154/contribDB_2004.csv.gz?rlkey=r3c01s6r9w08ju20d53pre1by&dl=1)
*   [2006 (contribDB_2006.csv.gz)](https://www.dropbox.com/scl/fi/rsdniza4ux83n9riqngxi/contribDB_2006.csv.gz?rlkey=rr0rua9e9xd6wfqz09z8jmq5v&dl=1)
*   [2008 (contribDB_2008.csv.gz)](https://www.dropbox.com/scl/fi/xqofzs1jshzzdksm7uror/contribDB_2008.csv.gz?rlkey=lrvar7w01ngeowjrjrlz2uoqw&dl=1)
*   [2010 (contribDB_2010.csv.gz)](https://www.dropbox.com/scl/fi/lyembrg3vmj3lzjzg3a62/contribDB_2010.csv.gz?rlkey=f4erj4h8fdq7pbqacb4spib3o&dl=1)
*   [2012 (contribDB_2012.csv.gz)](https://www.dropbox.com/scl/fi/dx8tafolqtrgp2dbn4fg6/contribDB_2012.csv.gz?rlkey=sslqxjhubk9745pfb0shcq5k4&dl=1)
*   [2014 (contribDB_2014.csv.gz)](https://www.dropbox.com/scl/fi/g0omy5h86mddmwcai43fk/contribDB_2014.csv.gz?rlkey=btee8x45og1vphwpvnfe9qttg&dl=1)
*   [2016 (contribDB_2016.csv.gz)](https://www.dropbox.com/scl/fi/qg5vezrx876cmu7u9hehr/contribDB_2016.csv.gz?rlkey=dsl4htd0ovr8hyn7xwctel0a0&dl=1)
*   [2018 (contribDB_2018.csv.gz)](https://www.dropbox.com/scl/fi/sk2fbjbrq7hgdqfnern2g/contribDB_2018.csv.gz?rlkey=qsk4o1wjc8p1bwozuuk4bwq01&dl=1)
*   [2020 (contribDB_2020.csv.gz)](https://www.dropbox.com/scl/fi/rnmdp79g0ewbf9j68tz1s/contribDB_2020.csv.gz?rlkey=v3y2xuvnmqueaiwkllls81mul&dl=1)
*   [2022 (contribDB_2022.csv.gz)](https://www.dropbox.com/scl/fi/odu6raws98gu1xdmx0ql3/contribDB_2022.csv.gz?rlkey=bvrmhaftpp2sa6tv3lu120v44&dl=1)
*   [2024 (contribDB_2024.csv.gz)](https://www.dropbox.com/scl/fi/p3adbtd50033ilt5ir3n2/contribDB_2024.csv.gz?rlkey=gt8l9j6xoi6h07syr94f33oyv&dl=1)

</details>

#### **Contribution Records Grouped by Office**
Gzipped CSV files containing every itemized donation for specific offices (1979-2024):

*   [President (contribDB_president.csv.gz)](https://www.dropbox.com/scl/fi/06j4afcl16ns1f8lxvya0/contribDB_president.csv.gz?rlkey=7px0di952y048qjo8fwa9bdt3&dl=1): All itemized contributions to presidential candidates.
*   [Governor (contribDB_governor.csv.gz)](https://www.dropbox.com/scl/fi/jkhypu6t1qs06r5k50xl0/contribDB_governor.csv.gz?rlkey=5xnv3frii98f4t5wrf312fjkg&dl=1): All itemized contributions to gubernatorial candidates.
*   [Judicial (contribDB_judicial.csv.gz)](https://www.dropbox.com/scl/fi/vfobrb0meg17nerut6crc/contribDB_judicial.csv.gz?rlkey=19k4w9vxbc0f1jars44m6e98l&dl=1): All itemized contributions to judicial candidates (state and local).

#### **Curated Datasets of Political Elites**
Standardized datasets containing ideology scores and biographic metadata for specific elite populations:

<details>
<summary>View Curated Elite Datasets</summary>

*   [Federal Court Judges](http://dx.doi.org/10.7910/DVN/RPZLMY): Campaign finance profiles and ideology scores for federal judges (Updated 2024).
*   [Fortune 500 Directors and CEOs](http://dx.doi.org/10.7910/DVN/6R1HAS): Political contribution history and scores for top corporate leadership.
*   [State Supreme Court Justices](http://web.stanford.edu/~bonica/files/bw_ssc_db.zip): Database of state supreme court justice ideology and campaign finance.
*   [Executive Appointees to Federal Agencies](http://web.stanford.edu/~bonica/files/bcj_data_and_replication_code.zip): Data on contributions and scores for federal agency appointees.
*   [Medical Professionals](http://web.stanford.edu/~bonica/files/brr_jama-im.zip): Curated dataset of political donations and ideology for healthcare professionals.

</details>

### 3. DIME PLUS (Legislative Voting Data)
*   **Description:** A refined subset of voting data curated by Adam Bonica for alignment with DIME ideology scores (107th–114th Congress).
*   **Data Provided:** Individual votes linked to specific bill IDs and Bonica recipient IDs.
*   **Download URLs:**
    *   [vote_db.csv](https://dataverse.harvard.edu/api/access/datafile/:persistentId/?persistentId=doi:10.7910/DVN/BO7WOW/F8RP2R&format=original): Transactional roll call vote records linked to DIME recipient IDs.
    *   [bills_db.csv](https://dataverse.harvard.edu/api/access/datafile/:persistentId/?persistentId=doi:10.7910/DVN/BO7WOW/F6IOAU&format=original): Metadata for bills and amendments including sponsor and co-sponsor lists.
    *   [text_db.csv](https://dataverse.harvard.edu/api/access/datafile/:persistentId/?persistentId=doi:10.7910/DVN/BO7WOW/C2VLCC&format=original): Parsed text from the Congressional Record for legislative speech analysis.

### 4. Congressional Bills Project (CBP)
*   **Description:** Manual coded topic classifications for U.S. congressional bills (1947–2016).
*   **Data Provided:** Bill IDs linked to the Policy Agendas Project (PAP) taxonomy.
*   **Download URL:** [Congressional Bills Dataset (CSV)](https://minio.la.utexas.edu/compagendas/datasetfiles/US-Legislative-congressional_bills_19.3_3_3%20%281%29.csv): Direct download for the master bill file (1947–2016) from the Comparative Agendas Project.

### 5. Congress.gov (Library of Congress / GPO)
*   **Description:** Official source for federal legislative information and parliamentary subject indexing.
*   **Data Provided:** Granular bill status metadata, CRS subject headings, and detailed sponsor/cosponsor info (2015–2024).
*   **Download URL:** [https://www.govinfo.gov/bulkdata/BILLSTATUS](https://www.govinfo.gov/bulkdata/BILLSTATUS): GPO portal for bulk XML downloads.

#### **Bill Status Bulk Data Structure**
The GPO provides Bill Status data in XML format, organized by Congress and bill type. Each bill type directory contains individual XML files for each bill, as well as a consolidated bulk ZIP file.

**Bulk ZIP URL Pattern:**
`https://www.govinfo.gov/bulkdata/BILLSTATUS/{Congress}/{BillType}/BILLSTATUS-{Congress}-{BillType}.zip`

`https://www.govinfo.gov/bulkdata/BILLSTATUS/119/hr/BILLSTATUS-119-hr.zip`

**Supported Bill Types:**
* `hr`: House Bills
* `hres`: House Resolutions
* And other types: `hjres`, `hconres`, `s`, `sres`, `sjres`, `sconres`.

<details>
<summary>View Bulk Data by Congress (108th–119th)</summary>

*   [119th Congress (2025–2026)](https://www.govinfo.gov/bulkdata/BILLSTATUS/119)
*   [118th Congress (2023–2024)](https://www.govinfo.gov/bulkdata/BILLSTATUS/118)
*   [117th Congress (2021–2022)](https://www.govinfo.gov/bulkdata/BILLSTATUS/117)
*   [116th Congress (2019–2020)](https://www.govinfo.gov/bulkdata/BILLSTATUS/116)
*   [115th Congress (2017–2018)](https://www.govinfo.gov/bulkdata/BILLSTATUS/115)
*   [114th Congress (2015–2016)](https://www.govinfo.gov/bulkdata/BILLSTATUS/114)
*   [113th Congress (2013–2014)](https://www.govinfo.gov/bulkdata/BILLSTATUS/113)
*   [112th Congress (2011–2012)](https://www.govinfo.gov/bulkdata/BILLSTATUS/112)
*   [111th Congress (2009–2010)](https://www.govinfo.gov/bulkdata/BILLSTATUS/111)
*   [110th Congress (2007–2008)](https://www.govinfo.gov/bulkdata/BILLSTATUS/110)
*   [109th Congress (2005–2006)](https://www.govinfo.gov/bulkdata/BILLSTATUS/109)
*   [108th Congress (2003–2004)](https://www.govinfo.gov/bulkdata/BILLSTATUS/108)

</details>

---

## Legislator & Entity Metadata

### 6. GitHub @unitedstates Project
*   **Description:** Community-maintained repository of U.S. legislative metadata.
*   **Data Provided:** Biographic profiles, term history, social media handles, and cross-reference IDs for current and historical Members of Congress.
*   **Download URLs:**
    *   [legislators-current.yaml](https://github.com/unitedstates/congress-legislators/blob/main/legislators-current.yaml): Metadata for all currently serving members.
    *   [legislators-historical.yaml](https://github.com/unitedstates/congress-legislators/blob/main/legislators-historical.yaml): Metadata for all members who have left office.

### 7. FEC (Federal Election Commission)
*   **Description:** Official federal campaign finance disclosure data.
*   **Data Provided:** Candidates, Committees, Contributions (Individuals, PACs), and Linkages.
*   **Download URL:** [https://www.fec.gov/data/browse-data/?tab=bulk-data](https://www.fec.gov/data/browse-data/?tab=bulk-data): FEC portal for weekly-updated bulk campaign finance archives.

#### **FEC Bulk Data Structure**
The FEC provides bulk data as gzipped ZIP files organized by election cycle (two-year periods ending in even years).

**Bulk Download URL Pattern:**
`https://www.fec.gov/files/bulk-downloads/[YEAR]/[PREFIX][YY].zip`

**Supported Data Types:**
*   **Candidate Master (`cn`)**: Basic candidate information and IDs.
*   **Committee Master (`cm`)**: Registered committee metadata and IDs.
*   **PAC to Candidate Contributions (`pas2`)**: Itemized contributions from committees to candidates.
*   **Committee to Committee Transactions (`oth`)**: Transfers and contributions between committees.
*   **Individual Contributions (`indiv`)**: Itemized receipts from individuals to committees.
*   **Candidate-Committee Linkage (`ccl`)**: Crosswalk linking candidates to their authorized committees.

<details>
<summary>View Bulk ZIP Links by Cycle (2012–2024)</summary>

| Cycle | Candidate Master | Committee Master | Individual Contribs | PAC to Candidate |
| :--- | :--- | :--- | :--- | :--- |
| **2024** | [cn24.zip](https://www.fec.gov/files/bulk-downloads/2024/cn24.zip) | [cm24.zip](https://www.fec.gov/files/bulk-downloads/2024/cm24.zip) | [indiv24.zip](https://www.fec.gov/files/bulk-downloads/2024/indiv24.zip) | [pas224.zip](https://www.fec.gov/files/bulk-downloads/2024/pas224.zip) |
| **2022** | [cn22.zip](https://www.fec.gov/files/bulk-downloads/2022/cn22.zip) | [cm22.zip](https://www.fec.gov/files/bulk-downloads/2022/cm22.zip) | [indiv22.zip](https://www.fec.gov/files/bulk-downloads/2022/indiv22.zip) | [pas222.zip](https://www.fec.gov/files/bulk-downloads/2022/pas222.zip) |
| **2020** | [cn20.zip](https://www.fec.gov/files/bulk-downloads/2020/cn20.zip) | [cm20.zip](https://www.fec.gov/files/bulk-downloads/2020/cm20.zip) | [indiv20.zip](https://www.fec.gov/files/bulk-downloads/2020/indiv20.zip) | [pas220.zip](https://www.fec.gov/files/bulk-downloads/2020/pas220.zip) |
| **2018** | [cn18.zip](https://www.fec.gov/files/bulk-downloads/2018/cn18.zip) | [cm18.zip](https://www.fec.gov/files/bulk-downloads/2018/cm18.zip) | [indiv18.zip](https://www.fec.gov/files/bulk-downloads/2018/indiv18.zip) | [pas218.zip](https://www.fec.gov/files/bulk-downloads/2018/pas218.zip) |
| **2016** | [cn16.zip](https://www.fec.gov/files/bulk-downloads/2016/cn16.zip) | [cm16.zip](https://www.fec.gov/files/bulk-downloads/2016/cm16.zip) | [indiv16.zip](https://www.fec.gov/files/bulk-downloads/2016/indiv16.zip) | [pas216.zip](https://www.fec.gov/files/bulk-downloads/2016/pas216.zip) |
| **2014** | [cn14.zip](https://www.fec.gov/files/bulk-downloads/2014/cn14.zip) | [cm14.zip](https://www.fec.gov/files/bulk-downloads/2014/cm14.zip) | [indiv14.zip](https://www.fec.gov/files/bulk-downloads/2014/indiv14.zip) | [pas214.zip](https://www.fec.gov/files/bulk-downloads/2014/pas214.zip) |
| **2012** | [cn12.zip](https://www.fec.gov/files/bulk-downloads/2012/cn12.zip) | [cm12.zip](https://www.fec.gov/files/bulk-downloads/2012/cm12.zip) | [indiv12.zip](https://www.fec.gov/files/bulk-downloads/2012/indiv12.zip) | [pas212.zip](https://www.fec.gov/files/bulk-downloads/2012/pas212.zip) |

</details>

### 8. Open States
*   **Description:** Comprehensive data on state-level legislators across all 50 states.
*   **Data Provided:** Legislator metadata (current and historical), contact info, party affiliation, and district data.
*   **Download URL:** [https://github.com/openstates/people](https://github.com/openstates/people): Source repository for state legislator metadata files.

#### Open States Bulk Data Formats
Open States (now Plural Policy) provides data in three primary formats:
1.  **YAML Repository (Full Archive):** [Download all as ZIP](https://github.com/openstates/people/archive/refs/heads/main.zip) (Contains all current and historical YAML files).
2.  **Nightly CSV Exports:** Per-state CSV files updated nightly.
3.  **PostgreSQL Database Dumps:** Monthly full database snapshots available at [data.openstates.org/postgres/monthly/](https://data.openstates.org/postgres/monthly/).

**CSV Download URL Pattern:**
`https://data.openstates.org/people/current/[ABBR].csv`

<details>
<summary>View Nightly CSV Downloads by State</summary>

| State | CSV Download Link | State | CSV Download Link |
| :--- | :--- | :--- | :--- |
| **Alabama** | [al.csv](https://data.openstates.org/people/current/al.csv) | **Montana** | [mt.csv](https://data.openstates.org/people/current/mt.csv) |
| **Alaska** | [ak.csv](https://data.openstates.org/people/current/ak.csv) | **Nebraska** | [ne.csv](https://data.openstates.org/people/current/ne.csv) |
| **Arizona** | [az.csv](https://data.openstates.org/people/current/az.csv) | **Nevada** | [nv.csv](https://data.openstates.org/people/current/nv.csv) |
| **Arkansas** | [ar.csv](https://data.openstates.org/people/current/ar.csv) | **New Hampshire** | [nh.csv](https://data.openstates.org/people/current/nh.csv) |
| **California** | [ca.csv](https://data.openstates.org/people/current/ca.csv) | **New Jersey** | [nj.csv](https://data.openstates.org/people/current/nj.csv) |
| **Colorado** | [co.csv](https://data.openstates.org/people/current/co.csv) | **New Mexico** | [nm.csv](https://data.openstates.org/people/current/nm.csv) |
| **Connecticut** | [ct.csv](https://data.openstates.org/people/current/ct.csv) | **New York** | [ny.csv](https://data.openstates.org/people/current/ny.csv) |
| **Delaware** | [de.csv](https://data.openstates.org/people/current/de.csv) | **North Carolina** | [nc.csv](https://data.openstates.org/people/current/nc.csv) |
| **Florida** | [fl.csv](https://data.openstates.org/people/current/fl.csv) | **North Dakota** | [nd.csv](https://data.openstates.org/people/current/nd.csv) |
| **Georgia** | [ga.csv](https://data.openstates.org/people/current/ga.csv) | **Ohio** | [oh.csv](https://data.openstates.org/people/current/oh.csv) |
| **Hawaii** | [hi.csv](https://data.openstates.org/people/current/hi.csv) | **Oklahoma** | [ok.csv](https://data.openstates.org/people/current/ok.csv) |
| **Idaho** | [id.csv](https://data.openstates.org/people/current/id.csv) | **Oregon** | [or.csv](https://data.openstates.org/people/current/or.csv) |
| **Illinois** | [il.csv](https://data.openstates.org/people/current/il.csv) | **Pennsylvania** | [pa.csv](https://data.openstates.org/people/current/pa.csv) |
| **Indiana** | [in.csv](https://data.openstates.org/people/current/in.csv) | **Rhode Island** | [ri.csv](https://data.openstates.org/people/current/ri.csv) |
| **Iowa** | [ia.csv](https://data.openstates.org/people/current/ia.csv) | **South Carolina** | [sc.csv](https://data.openstates.org/people/current/sc.csv) |
| **Kansas** | [ks.csv](https://data.openstates.org/people/current/ks.csv) | **South Dakota** | [sd.csv](https://data.openstates.org/people/current/sd.csv) |
| **Kentucky** | [ky.csv](https://data.openstates.org/people/current/ky.csv) | **Tennessee** | [tn.csv](https://data.openstates.org/people/current/tn.csv) |
| **Louisiana** | [la.csv](https://data.openstates.org/people/current/la.csv) | **Texas** | [tx.csv](https://data.openstates.org/people/current/tx.csv) |
| **Maine** | [me.csv](https://data.openstates.org/people/current/me.csv) | **Utah** | [ut.csv](https://data.openstates.org/people/current/ut.csv) |
| **Maryland** | [md.csv](https://data.openstates.org/people/current/md.csv) | **Vermont** | [vt.csv](https://data.openstates.org/people/current/vt.csv) |
| **Massachusetts** | [ma.csv](https://data.openstates.org/people/current/ma.csv) | **Virginia** | [va.csv](https://data.openstates.org/people/current/va.csv) |
| **Michigan** | [mi.csv](https://data.openstates.org/people/current/mi.csv) | **Washington** | [wa.csv](https://data.openstates.org/people/current/wa.csv) |
| **Minnesota** | [mn.csv](https://data.openstates.org/people/current/mn.csv) | **West Virginia** | [wv.csv](https://data.openstates.org/people/current/wv.csv) |
| **Mississippi** | [ms.csv](https://data.openstates.org/people/current/ms.csv) | **Wisconsin** | [wi.csv](https://data.openstates.org/people/current/wi.csv) |
| **Missouri** | [mo.csv](https://data.openstates.org/people/current/mo.csv) | **Wyoming** | [wy.csv](https://data.openstates.org/people/current/wy.csv) |

</details>

### 9. United States Governors (1775–2020)
*   **Description:** A comprehensive historical dataset of U.S. state and territorial governors, including biographical information, party affiliation, and tenure dates.
*   **Provider:** Jacob Kaplan
*   **Data Provided:** Governor names, state, party affiliation, and yearly served indicators (year-expanded CSV).
*   **Download URLs:**
    *   [Full CSV (Dataverse)](https://dataverse.harvard.edu/api/access/datafile/10734323?format=original): Direct download for the year-expanded CSV file.
    *   [Open ICPSR Project](https://www.openicpsr.org/openicpsr/project/102000/version/V3/view): Official project page for historical versions and metadata.
    *   [Harvard Dataverse Project](https://doi.org/10.7910/DVN/RYY3OW): Documentation and alternative access.

**Citation:**
> Kaplan, Jacob. United States Governors 1775-2020. Ann Arbor, MI: Inter-university Consortium for Political and Social Research [distributor], 2020-07-01. https://doi.org/10.3886/E102000V3

---

## Supplemental Classification Sources (Entity Verification)

These specialized datasets are used to explicitly classify contribution donors as labor unions, corporations, or non-profits.

### DOL (Department of Labor) - Union Disclosures
*   **Description:** Annual financial disclosure reports (LM-2, LM-3, LM-4) filed by labor unions.
*   **Data Provided:** Officer information, membership counts, and detailed financial receipts/disbursements.
*   **Download URL:** [OLMS Yearly Data Download](https://olmsapps.dol.gov/olpdr/#Union%20Reports/Yearly%20Data%20Download/): Direct manual portal for annual pipe-delimited ZIP files.

### IRS (Non-profits) - EO BMF and Form 990
*   **Description:** Metadata and financial snapshots for tax-exempt organizations.
*   **Data Provided:** EINs (Tax IDs), organization names, and Form 990 filing indices.
*   **Download URLs:**
    *   **EO BMF (Northeast):** [eo1.csv](https://www.irs.gov/pub/irs-soi/eo1.csv)
    *   **EO BMF (Mid-Atlantic):** [eo2.csv](https://www.irs.gov/pub/irs-soi/eo2.csv)
    *   **EO BMF (Gulf Coast):** [eo3.csv](https://www.irs.gov/pub/irs-soi/eo3.csv)
    *   **EO BMF (Great Lakes):** [eo4.csv](https://www.irs.gov/pub/irs-soi/eo4.csv)
    *   **Form 990 Index (AWS Mirror):** [index.json](https://s3.amazonaws.com/irs-form-990/index.json)

### IRS Political (527 Organizations)
*   **Description:** Registration and disclosure filings for 527 political organizations.
*   **Data Provided:** Form 8871 (Registration) and Form 8872 (Contributions/Expenditures).
*   **Download URL:** [Full Bulk Data Download](https://forms.irs.gov/app/pod/dataDownload/fullData): A single compressed file containing all electronically filed data.

### LDA (Lobbying Disclosure Act)
*   **Description:** Federal lobbying activity reports filed with the Senate and House.
*   **Data Provided:** Itemized political contributions (LD-203) made by lobbyists.
*   **Download URLs:**
    *   **Itemized Contributions API:** [https://lda.senate.gov/api/v1/contributions/](https://lda.senate.gov/api/v1/contributions/)
    *   **Senate Bulk Filings API:** [https://lda.senate.gov/api/v1/filings/](https://lda.senate.gov/api/v1/filings/)

### SEC (Corporate) - CIK/Ticker Mappings
*   **Description:** Official mappings between corporate entities, their SEC Central Index Keys (CIKs), and stock tickers.
*   **Data Provided:** JSON and text-based crosswalks for major public filers.
*   **Download URLs:**
    *   [company_tickers.json](https://www.sec.gov/files/company_tickers.json)
    *   [ticker.txt](https://www.sec.gov/include/ticker.txt)

### GLEIF (Legal Entity Identifier)
*   **Description:** Global standard for uniquely identifying legal entities participating in financial transactions.
*   **Data Provided:** "Golden Copy" of all Legal Entity Identifiers (LEIs) and associated reference data.
*   **Download URL:** [Daily Golden Copy (CSV ZIP)](https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv.zip): The most recent daily snapshot.
