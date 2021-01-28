# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Pandas, SQL, and the Grammar of Data
# %% [markdown]
# We'd like a data structure that can represent the columns in the data above by their name. In particular, we want a structure that can easily store variables of different types, that stores column names, and that we can reference by column name as well as by indexed position.  And it would be nice this data structure came with built-in functions that we can use to manipulate it. 
# 
# Pandas is a package/library that does all of this!  The library is built on top of numpy.  There are two basic pandas objects, *series* and *dataframes*, which can be thought of as enhanced versions of 1D and 2D numpy arrays, respectively.  Indeed Pandas attempts to keep all the efficiencies that `numpy` gives us.
# 
# For reference, here is a useful pandas [cheat sheet](https://drive.google.com/folderview?id=0ByIrJAE4KMTtaGhRcXkxNHhmY2M&usp=sharing) and the pandas [documentation](https://pandas.pydata.org/pandas-docs/stable/).

# %%
import pandas as pd


# %%
from collections import OrderedDict
def make_frame(list_of_tuples, legend):
    framelist=[]
    for i, cname in enumerate(legend):
        framelist.append((cname,[e[i] for e in list_of_tuples]))
    return pd.DataFrame.from_dict(OrderedDict(framelist)) 

# %% [markdown]
# ## Pandas
# %% [markdown]
# Here is what the data looks like from the file `data/candidates.txt`
# 
# ```
# id|first_name|last_name|middle_name|party
# 33|Joseph|Biden||D
# 36|Samuel|Brownback||R
# 34|Hillary|Clinton|R.|D
# 39|Christopher|Dodd|J.|D
# 26|John|Edwards||D
# 22|Rudolph|Giuliani||R
# 24|Mike|Gravel||D
# 16|Mike|Huckabee||R
# 30|Duncan|Hunter||R
# 31|Dennis|Kucinich||D
# 37|John|McCain||R
# 20|Barack|Obama||D
# 32|Ron|Paul||R
# 29|Bill|Richardson||D
# 35|Mitt|Romney||R
# 38|Tom|Tancredo||R
# 41|Fred|Thompson|D.|R
# ```
# %% [markdown]
# ### Building a dataframe
# 
# The easiest way to build a dataframe is simply to read in a CSV file. 
# 
# This example is adapted from: https://github.com/tthibo/SQL-Tutorial
# %% [markdown]
# Use Pandas to read in the data

# %%
dfcand=pd.read_csv("data/candidates.txt", sep='|')
dfcand

# %% [markdown]
# Here is the other file, of contributions to candidates:
# 
# ```
# id|last_name|first_name|middle_name|street_1|street_2|city|state|zip|amount|date|candidate_id
# |Agee|Steven||549 Laurel Branch Road||Floyd|VA|24091|500.00|2007-06-30|16
# |Ahrens|Don||4034 Rennellwood Way||Pleasanton|CA|94566|250.00|2007-05-16|16
# |Ahrens|Don||4034 Rennellwood Way||Pleasanton|CA|94566|50.00|2007-06-18|16
# |Ahrens|Don||4034 Rennellwood Way||Pleasanton|CA|94566|100.00|2007-06-21|16
# |Akin|Charles||10187 Sugar Creek Road||Bentonville|AR|72712|100.00|2007-06-16|16
# |Akin|Mike||181 Baywood Lane||Monticello|AR|71655|1500.00|2007-05-18|16
# |Akin|Rebecca||181 Baywood Lane||Monticello|AR|71655|500.00|2007-05-18|16
# |Aldridge|Brittni||808 Capitol Square Place, SW||Washington|DC|20024|250.00|2007-06-06|16
# |Allen|John D.||1052 Cannon Mill Drive||North Augusta|SC|29860|1000.00|2007-06-11|16
# |Allen|John D.||1052 Cannon Mill Drive||North Augusta|SC|29860|1300.00|2007-06-29|16
# |Allison|John W.||P.O. Box 1089||Conway|AR|72033|1000.00|2007-05-18|16
# |Allison|Rebecca||3206 Summit Court||Little Rock|AR|72227|1000.00|2007-04-25|16
# |Allison|Rebecca||3206 Summit Court||Little Rock|AR|72227|200.00|2007-06-12|16
# |Altes|R.D.||8600 Moody Road||Fort Smith|AR|72903|2300.00|2007-06-21|16
# |Andres|Dale||1160 Glen Oaks Drive||West Des Moines|IA|50266|250.00|2007-06-06|16
# |Anthony|John||211 Long Island Drive||Hot Springs|AR|71913|2300.00|2007-06-12|16
# |Arbogast|Robert||12900 State Route 56 SE||Mount Sterling|OH|43143|500.00|2007-04-08|16
# |Arbogast|Robert||12900 State Route 56 SE||Mount Sterling|OH|43143|100.00|2007-06-22|16
# |Ardle|William||412 Dakota Avenue||Springfield|OH|45504|50.00|2007-06-28|16
# |Atiq|Omar||7200 S Hazel Street||Pine Bluff|AR|71603|1000.00|2007-05-18|16
# |Atiq|Omar||7200 S Hazel Street||Pine Bluff|AR|71603|1000.00|2007-06-27|16
# |Baker|David||2550 Adamsbrooke Drive||Conway|AR|72034|2300.00|2007-04-11|16
# |Bancroft|David||2934 Broderick Street||San Francisco|CA|94123|250.00|2007-04-24|16
# |Banks|Charles||P.O. Box 251310||Little Rock|AR|72225|1000.00|2007-05-14|16
# |Barbee|John||516 Kellyridge Drive||Apex|NC|27502|500.00|2007-05-23|16
# |Buckler|Steve||24351 Armada Dr||Dana Point|CA|926291306|50.00|2007-07-30|20
# |Buckler|Steve||24351 Armada Dr||Dana Point|CA|926291306|25.00|2007-08-16|20
# |Buckheit|Bruce||8904 KAREN DR||FAIRFAX|VA|220312731|100.00|2007-09-19|20
# |Buckel|Linda||PO Box 683130||Park City|UT|840683130|2300.00|2007-08-14|20
# |Buckel|Linda||PO Box 683130||Park City|UT|840683130|-2300.00|2007-08-14|20
# |Buckel|Linda||PO Box 683130||Park City|UT|840683130|4600.00|2007-08-14|20
# |Buck|Thomas||4206 Terrace Street||Kansas City|MO|64111|100.00|2007-09-25|20
# |Buck|Jay|K.|1855 Old Willow Rd Unit 322||Northfield|IL|600932918|200.00|2007-09-12|20
# |Buck|Blaine|M|45 Eaton Ave||Camden|ME|048431752|2300.00|2007-09-30|20
# |Buck|Barbara||1780 NE 138th St||North Miami|FL|331811316|50.00|2007-09-13|20
# |Buck|Barbara||1780 NE 138th St||North Miami|FL|331811316|50.00|2007-07-19|20
# |Buchman|Mark M||2530 Lawton Ave||San Luis Obispo|CA|934015622|460.80|2007-07-18|20
# |Bucher|Ida|M|1400 Warnall Ave||Los Angeles|CA|900245333|100.00|2007-07-10|20
# |Buchanek|Elizabeth||7917 Kentbury Dr||Bethesda|MD|208144615|50.00|2007-09-30|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|500.00|2007-09-24|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|-500.00|2007-09-24|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|500.00|2007-09-24|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|700.00|2007-08-28|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|-700.00|2007-08-28|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|1000.00|2007-08-28|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|1300.00|2007-08-09|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|200.00|2007-08-14|20
# |Buchanan|John||2025 NW 29th Rd||Boca Raton|FL|334316303|500.00|2007-07-25|20
# |Buchanan|John||4635 49th St NW||Washington|DC|200164320|200.09|2007-09-23|20
# |Harrison|Ryan||2247 3rd St||La Verne|CA|917504918|25.00|2007-07-26|20
# |BYNUM|HERBERT||332 SUNNYSIDE ROAD||TAMPA|FL|336177249|-500.00|2008-03-10|22
# |BYINGTON|MARGARET|E.|2633 MIDDLEBORO LANE N.E.||GRAND RAPIDS|MI|495061254|-2300.00|2008-03-03|22
# |BYERS|BOB|A.|13170 TELFAIR AVENUE||SYLMAR|CA|913423573|-2300.00|2008-03-07|22
# |BYERS|AUDREY||2658 LADBROOK WAY||THOUSAND OAKS|CA|913615073|-200.00|2008-03-07|22
# |BUSH|KRYSTIE||P.O. BOX 61046||DENVER|CO|802061046|-2300.00|2008-03-06|22
# |BUSH|ERIC||P.O. BOX 61046||DENVER|CO|802061046|-2300.00|2008-03-06|22
# |BURTON|SUSAN||9338 DEER CREEK DRIVE||TAMPA|FL|336472286|-2300.00|2008-03-05|22
# |BURTON|STEVEN|G.|9938 DEER CREEK DRIVE||TAMPA|FL|33647|-2300.00|2008-03-05|22
# |BURTON|GLENN|M.|4404 CHARLESTON COURT||TAMPA|FL|336092620|-2300.00|2008-03-05|22
# |BURKHARDT|CRAIG|S.|910 15TH STREET N.W.||WASHINGTON|DC|200052503|-500.00|2008-03-07|22
# |BURKHARDT|CRAIG|S.|910 15TH STREET N.W.||WASHINGTON|DC|200052503|-1000.00|2008-03-07|22
# |BURKHARDT|BARBARA||910 15TH STREET N.W.||WASHINGTON|DC|200052503|-500.00|2008-03-07|22
# |BURKE|SUZANNE|M.|3401 EVANSTON||SEATTLE|WA|981038677|-700.00|2008-03-05|22
# |BURKE|GAIL||165 E. 32ND STREET|APARTMENT 9E|NEW YORK|NY|100166014|-2000.00|2008-03-05|22
# |BURKE|DONALD|J.|12 LOMPOC||RANCHO SANTA MARGA|CA|926881817|-2300.00|2008-03-11|22
# |BURGERT|RONALD|L.|5723 PLUMTREE DRIVE||DALLAS|TX|752524926|-1000.00|2008-03-05|22
# |BULL|BARTLE|B.|439 E. 51ST STREET||NEW YORK|NY|100226473|-800.00|2008-03-10|22
# |BULL|BARTLE|B.|439 E. 51ST STREET||NEW YORK|NY|100226473|-1000.00|2008-03-10|22
# |BUKOWSKI|DANIEL|J.|702 S. WRIGHT STREET||NAPERVILLE|IL|605406736|-100.00|2008-03-10|22
# |BUISSON|MARGARET|A.|P.O. BOX 197029||LOUISVILLE|KY|402597029|-200.00|2008-03-11|22
# |BUCKLEY|WALTER|W.|1635 COUNTRY ROAD||BETHLEHEM|PA|180155718|-100.00|2008-03-05|22
# |BUCKLEY|MARJORIE|B.|1635 COUNTRY ROAD||BETHLEHEM|PA|180155718|-100.00|2008-03-05|22
# |BRUNO|JOHN||10136 WINDERMERE CHASE BLVD.||GOTHA|FL|347344707|-2300.00|2008-03-06|22
# |BRUNO|IRENE||10136 WINDERMERE CHASE BLVD.||GOTHA|FL|347344707|-2300.00|2008-03-06|22
# |BROWN|TIMOTHY|J.|26826 MARLOWE COURT||STEVENSON RANCH|CA|913811020|-2300.00|2008-03-06|22
# |Schuff|Bryan||1700 W Sweden Rd||Brockport|NY|14420|-25.00|2008-08-22|32
# |Hobbs|James||229 Cherry Lane||White House|TN|37188|-25.00|2008-08-19|32
# |Ranganath|Anoop||2507 Willard Drive||Charlottesville|VA|22903|-100.00|2008-04-21|32
# |Nystrom|Michael|A|93A Fairmont Street||Arlington|MA|02474|-503.00|2008-04-21|32
# |Muse|Nina|Jo|2915 Toro Canyon Rd||Austin|TX|78746|-50.00|2008-04-21|32
# |Waddell|James|L.|1823 Spel Lane SW||Rochester|MN|55902|-28.00|2008-04-21|32
# |Brucks|William|C.|PO Box 391||Corona del Mar|CA|92625|-150.00|2008-04-21|32
# |Kuehn|David||14502 West 93rd Street||Lenexa|KS|66215|-330.00|2008-04-21|32
# |Verster|Jeanette|M.|7220 SW 61st St||Miami|FL|331431807|-1000.00|2008-04-21|32
# |Uihlein|Richard||1396 N Waukegan Rd||Lake Forest|IL|600451147|-2300.00|2008-04-21|32
# |Eskenberry|Robert|P|10960 Gray Cir||Westminster|CO|80020|-223.00|2008-04-21|32
# |Froehling|Alan|L.|302 Broadway St||Mount Vernon|IL|628645116|-844.80|2008-04-21|32
# |Duryea|Marcia|A.|123 Bayview Ave||Amityville|NY|11701|-299.50|2008-04-21|32
# |Perreault|Louise||503 Brockridge Hunt Drive||Hampton|VA|23666|-34.08|2008-04-21|32
# |Rozenfeld|Timur||57 Herbert Road||Robbinsville|NJ|08691|-777.95|2008-04-21|32
# |Kazor|Christopher|M|707 Spindletree ave||Naperville|IL|60565|-2592.00|2008-04-21|32
# |Lehner|Thomas|S.|2701 Star Lane||Wadsworth|OH|44281|-200.00|2008-04-21|32
# |Plummer|Joseph||587 Blake Hill Rd||New Hampton|NH|032564424|-24.60|2008-04-21|32
# |Raught|Philip|M|4714 Plum Way||Pittsburgh|PA|15201|-1046.00|2008-04-21|32
# |Ferrara|Judith|D|1508 Waterford Road||Yardley|PA|19067|-1100.00|2008-04-21|32
# |Johnson|Cathleen|E.|1003 Justin Ln Apt 2016||Austin|TX|787572648|-14.76|2008-04-21|32
# |Sanford|Bradley||940 Post St #43||San Francisco|CA|94109|-24.53|2008-04-21|32
# |Gaarder|Bruce||PO Box 4085||Mountain Home AFB|ID|83648|-261.00|2008-04-21|32
# |Choe|Hyeokchan||207 Bridle Way||Fort Lee|NJ|070246302|-39.50|2008-04-21|32
# |Jacobs|Richard|G.|14337 Tawya Rd||Apple Valley|CA|923075545|-1000.00|2008-04-21|32
# |Aaronson|Rebecca||2000 Village Green Dr Apt 12||Mill Creek|WA|980125787|100.00|2008-02-08|34
# |Aarons|Elaine||481 Buck Island Rd Apt 17A|APT 17A|West Yarmouth|MA|026733300|25.00|2008-02-26|34
# |Aarons|Elaine||481 Buck Island Rd Apt 17A|APT 17A|West Yarmouth|MA|026733300|70.00|2008-02-25|34
# |Aarons|Elaine||481 Buck Island Rd Apt 17A|APT 17A|West Yarmouth|MA|026733300|100.00|2008-02-08|34
# |Aaron|Shirley||101 Cherry Ave||Havana|FL|323331311|50.00|2008-02-29|34
# |Aaron|Shirley||101 Cherry Ave||Havana|FL|323331311|100.00|2008-02-29|34
# |Aaronson|Rebecca||2000 Village Green Dr Apt 12||Mill Creek|WA|980125787|100.00|2008-02-14|34
# |Aaron|Shirley||101 Cherry Ave||Havana|FL|323331311|100.00|2008-02-24|34
# |Aaron|Shirley||101 Cherry Ave||Havana|FL|323331311|100.00|2008-02-22|34
# |Aaron|Shirley||101 Cherry Ave||Havana|FL|323331311|100.00|2008-02-17|34
# |Reid|Elizabeth||73 W Patent Rd|OPHIR FARM NORTH|Bedford Hills|NY|105072222|-350.00|2008-08-28|34
# |Reich|Thomas||499 Park Ave||New York|NY|100221240|-2300.00|2008-08-28|34
# |Aaron|Shirley||101 Cherry Ave||Havana|FL|323331311|100.00|2008-02-08|34
# |Aaron|Shirley||101 Cherry Ave||Havana|FL|323331311|100.00|2008-02-03|34
# |Aaron|Sharron||1804 E Montgomery St||Broken Arrow|OK|740121840|500.00|2008-02-09|34
# |Aaron|Patricia||418 NW 35th St||Oklahoma City|OK|731188602|200.00|2008-02-26|34
# |Aaron|Patricia||418 NW 35th St||Oklahoma City|OK|731188602|100.00|2008-02-12|34
# |Aaron|Jim||2178 Fairway Cir||Canton|MI|481885097|300.00|2008-02-07|34
# |Aaron|Jim||2178 Fairway Cir||Canton|MI|481885097|200.00|2008-02-29|34
# |Aaron|Carole||PO Box 1806||Ogunquit|ME|039071806|70.00|2008-02-29|34
# |Aaron|Carole||PO Box 1806||Ogunquit|ME|039071806|50.00|2008-02-07|34
# |Aaron|Carole||PO Box 1806||Ogunquit|ME|039071806|100.00|2008-02-03|34
# |Aaron|Barbara||2298 Pacific Ave # 6||San Francisco|CA|941151435|1000.00|2008-02-11|34
# |Aanonsen|Lin||897 Raymond Ave||Saint Paul|MN|551141508|250.00|2008-02-21|34
# |Aanonsen|Lin||897 Raymond Ave||Saint Paul|MN|551141508|100.00|2008-02-08|34
# |BOURNE|TRAVIS||LAGE KAART 77||BRASSCHATT||02930|-500.00|2008-11-20|35
# |SECRIST|BRIAN|L.|3 MULE DEER TRAIL||LITTLETON|CO|801275722|-1000.00|2008-04-07|35
# |TOLLESTRUP|TRAVIS|W.|16331 WINECREEK RD.||SAN DIEGO|CA|92127|-1000.00|2008-05-15|35
# |ACCORD|DEAN|C.|8813 ROBINSON RIDGE ROAD||LAS VEGAS|NV|891175812|500.00|2007-07-13|35
# |ABTS|HENRY||P. O. BOX 7299||INCLINE VILLAGE|NV|894527299|100.00|2007-07-13|35
# |ABSHIER|LANNY||14191 S.E. HIGHWAY 301||SUMMERFIELD|FL|34491|500.00|2007-09-25|35
# |ABSHIER|DIANA||14191 S.E. HIGHWAY 301||SUMMERFIELD|FL|34491|500.00|2007-09-25|35
# |ABREU|KEVIN|M.|1305 GARDEN GLEN LANE||PEARLAND|TX|775816547|50.00|2007-09-30|35
# |ABREU|KEVIN|M.|1305 GARDEN GLEN LANE||PEARLAND|TX|775816547|150.00|2007-08-09|35
# |ABREU|KEVIN|M.|1305 GARDEN GLEN LANE||PEARLAND|TX|775816547|50.00|2007-07-19|35
# |ABRAMOWITZ|NIRA||411 HARBOR ROAD||SOUTHPORT|CT|068901376|2300.00|2007-09-14|35
# |ABRAMS|MICHAEL||7910 WOODMONT AVENUE||BETHESDA|MD|208143002|250.00|2007-09-29|35
# |ABRAMOWITZ|KEN||200 CENTRAL PARK S.|APARTMENT 31A|NEW YORK|NY|100191448|300.00|2007-09-11|35
# |ABOUBAKARE|NASAR||1400 SAN MIGUEL DRIVE||CORONA DEL MAR|CA|926251300|1000.00|2007-07-09|35
# |ABEGG|PATRICIA|T.|1862 E. 5150 S.||SALT LAKE CITY|UT|841176911|75.00|2007-09-25|35
# |ABEGG|PATRICIA|T.|1862 E. 5150 S.||SALT LAKE CITY|UT|841176911|25.00|2007-09-17|35
# |ABEGG|PATRICIA|T.|1862 E. 5150 S.||SALT LAKE CITY|UT|841176911|75.00|2007-08-31|35
# |ABEGG|PATRICIA|T.|1862 E. 5150 S.||SALT LAKE CITY|UT|841176911|75.00|2007-08-14|35
# |ABEGG|PATRICIA|T.|1862 E. 5150 S.||SALT LAKE CITY|UT|841176911|25.00|2007-08-06|35
# |ABEGG|PATRICIA|T.|1862 E. 5150 S.||SALT LAKE CITY|UT|841176911|25.00|2007-07-10|35
# |ABDELLA|THOMAS|M.|4231 MONUMENT WALL WAY #340||FAIRFAX|VA|220308440|50.00|2007-09-30|35
# |ABBOTT|WELDON|S.|777 EAST SOUTH TEMPLE  4E||SALT LAKE CITY|UT|841021269|100.00|2007-09-29|35
# |ABBOTT|WELDON|S.|777 EAST SOUTH TEMPLE  4E||SALT LAKE CITY|UT|841021269|50.00|2007-08-09|35
# |ABBOTT|GERALD|F.|389 BENEFIT STREET||PROVIDENCE|RI|029032946|100.00|2007-09-15|35
# |ABBOTT|GERALD|F.|389 BENEFIT STREET||PROVIDENCE|RI|029032946|100.00|2007-08-15|35
# |ABEDIN|ZAINUL||715 N. CENTRAL AVENUE|SUITE 212|GLENDALE|CA|912031164|500.00|2008-01-21|37
# |ABBOTT|SYBIL|F.|446 GAMES DRIVE||RENO|NV|895093326|75.00|2008-01-08|37
# |ABBOTT|SYBIL|F.|446 GAMES DRIVE||RENO|NV|895093326|50.00|2008-01-08|37
# |ABBOTT|RONALD|LEANDER|5453 HAWTHORNE STREET||MONTCLAIR|CA|917632551|200.00|2008-01-31|37
# |ABBOTT|RONALD|LEANDER|5453 HAWTHORNE STREET||MONTCLAIR|CA|917632551|100.00|2008-01-08|37
# |ABBOTT|ROBERT|A.|3061 LOREE ROAD||DECKERVILLE|MI|484279763|500.00|2008-01-21|37
# |ABBOTT|MIKE|E.|4516 OSPREY LNDG||NICEVILLE|FL|325786810|1000.00|2008-01-15|37
# |ABBOT|DAVID|M.|56 SALEM STREET||ANDOVER|MA|018102114|200.00|2008-01-21|37
# |ABBO|PAULINE|MORENCY|10720 JACOB LANE||WHITE LAKE|MI|483862274|35.00|2008-01-07|37
# |ABATE|MARIA|ELENA|1291 NIGHTINGALE AVENUE||MIAMI SPRINGS|FL|331663832|2600.00|2008-01-25|37
# |ABAIR|PETER||40 EVANS STREET||WATERTOWN|MA|024722150|25.00|2008-01-09|37
# |ABACHERLI|SHIRLEY|M.|29875 NEWPORT ROAD||MENIFEE|CA|925849524|150.00|2008-01-28|37
# |AARONS|CHARLES||1730 SHORE DRIVE||ANCHORAGE|AK|995153207|300.00|2008-01-30|37
# |AARONS|CHARLES||1730 SHORE DRIVE||ANCHORAGE|AK|995153207|410.00|2008-01-15|37
# |AARONS|CHARLES||1730 SHORE DRIVE||ANCHORAGE|AK|995153207|500.00|2008-01-09|37
# |ABEL|JOHN|H.|422 THOMAS STREET||BETHLEHEM|PA|180153316|200.00|2008-01-22|37
# |ABEL|MARLING|L.|14 HANGING MOSS LANE||GREENVILLE|SC|296155069|100.00|2008-01-22|37
# |ABEL|RUDOLPH||4532 OCEAN BLVD.|# 108|SARASOTA|FL|342421337|100.00|2008-01-08|37
# |ABELE|RODNEY||3620 METAIRIE HEIGHTS AVENUE||METAIRIE|LA|700021823|500.00|2008-01-15|37
# |ABERCROMBIE|DENIS||11811 WATER OAK CT||MAGNOLIA|TX|773546270|500.00|2008-01-30|37
# |ABESHAUS|MERRILL|M.|1801 N. HEREFORD DRIVE||FLAGSTAFF|AZ|860011121|120.00|2008-01-16|37
# |ABRAHAM|GEORGE||P.O. BOX 1504||LAKE CHARLES|LA|706021504|800.00|2008-01-17|37
# |ABRAHAMSON|PETER|J.|1030 W. ROSCOE STREET||CHICAGO|IL|606572207|50.00|2008-01-25|37
# |ABRAHAM|SALEM|A.|P.O. BOX 7||CANADIAN|TX|790140007|1000.00|2008-01-17|37
# |ABRAHAM|SALEM|A.|P.O. BOX 7||CANADIAN|TX|790140007|1300.00|2008-01-30|37
# ```

# %%
dfcwci=pd.read_csv("data/contributors_with_candidate_id.txt", sep="|")
dfcwci.head()

# %% [markdown]
# You'll notice that the contributions dont have the first column, so we will need to be cleaning things up a bit...

# %%
del dfcwci['id']
dfcwci.head()

# %% [markdown]
# ## SQL and Relational Databases
# %% [markdown]
# Lets start with Relational Databases, so called because they contain "relations" (tables), which are SETS of "tuples" (rows) which map "attributes" (columns) to atomic values.
# 
# The available attributes are constrained by a "header" tuple of attributes which set the type. We do this below here, using the SQL language to set things up.
# %% [markdown]
# ```sql
# DROP TABLE IF EXISTS "candidates";
# DROP TABLE IF EXISTS "contributors";
# CREATE TABLE "candidates" (
#     "id" INTEGER PRIMARY KEY  NOT NULL ,
#     "first_name" VARCHAR,
#     "last_name" VARCHAR,
#     "middle_name" VARCHAR,
#     "party" VARCHAR NOT NULL
# );
# CREATE TABLE "contributors" (
#     "id" INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL,
#     "last_name" VARCHAR,
#     "first_name" VARCHAR,
#     "middle_name" VARCHAR,
#     "street_1" VARCHAR,
#     "street_2" VARCHAR,
#     "city" VARCHAR,
#     "state" VARCHAR,
#     "zip" VARCHAR, -- Notice that we are converting the zip from integer to string
#     "amount" INTEGER,
#     "date" DATETIME,
#     "candidate_id" INTEGER NOT NULL,
#     FOREIGN KEY(candidate_id) REFERENCES candidates(id)
# );
# ```

# %%
ourschema="""
DROP TABLE IF EXISTS "candidates";
DROP TABLE IF EXISTS "contributors";
CREATE TABLE "candidates" (
    "id" INTEGER PRIMARY KEY  NOT NULL ,
    "first_name" VARCHAR,
    "last_name" VARCHAR,
    "middle_name" VARCHAR,
    "party" VARCHAR NOT NULL
);
CREATE TABLE "contributors" (
    "id" INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL,
    "last_name" VARCHAR,
    "first_name" VARCHAR,
    "middle_name" VARCHAR,
    "street_1" VARCHAR,
    "street_2" VARCHAR,
    "city" VARCHAR,
    "state" VARCHAR,
    "zip" VARCHAR, -- Notice that we are converting the zip from integer to string
    "amount" INTEGER,
    "date" DATETIME,
    "candidate_id" INTEGER NOT NULL,
    FOREIGN KEY(candidate_id) REFERENCES candidates(id)
);
"""

# %% [markdown]
# ### SQLITE
# %% [markdown]
# We use sqlite here (and recommend Postgres for production purposes). Sqlite is a on-file database, as opposed to other common databases such as Oracle and Psogres, which run as different processes on your system. Sqlite is great for on-disk large databases which wont fit into memory. 
# 
# Its also built into Python, but to use the [command line tool](https://www.sqlite.org/cli.html), I recommend you install it: https://www.sqlite.org/download.html. I also recommend you download and install the sqlite browser: http://sqlitebrowser.org .
# 
# Python implements a standard database API over all databases. Its called [DBAPI2](http://cewing.github.io/training.codefellows/lectures/day21/intro_to_dbapi2.html). It works across many SQL databases.
# 
# There is an even higher level API available, called [SQLAlchemy](http://www.sqlalchemy.org). While we wont use it here, I thoroughly recommend it. Many things in Pandas use it to interface with databases. Here we'll get away with things by using SQLITE.
# %% [markdown]
# We'll write some functions to connect to Sqlite.
# 
# (1) Connect and get a DBAPI2 connection.

# %%
from sqlite3 import dbapi2 as sq3
from pathlib import Path
PATHSTART="."
def get_db(dbfile):
    sqlite_db = sq3.connect(Path(PATHSTART) / dbfile)
    return sqlite_db

# %% [markdown]
# (2) Set up the database with tables. Drop tables if they exist and create them.

# %%
def init_db(dbfile, schema):
    """Creates the database tables."""
    db = get_db(dbfile) 
    db.cursor().executescript(schema)
    db.commit()
    return db

# %% [markdown]
# Initializing the database:

# %%
db=init_db("/tmp/cancont.db", ourschema)

# %% [markdown]
# ### Populating with Pandas!!

# %%
dfcand.to_sql("candidates", db, if_exists="append", index=False)


# %%
dfcwci.to_sql("contributors", db, if_exists="append", index=False)

# %% [markdown]
# Now we can do queries in the lingua franca of databases: SQL

# %%
sel="""
SELECT * FROM candidates;
"""
c=db.cursor().execute(sel)


# %%
c.fetchall()


# %%
def make_query(sel):
    c=db.cursor().execute(sel)
    return c.fetchall()


# %%
make_query("SELECT * FROM candidates;")


# %%
make_query("PRAGMA table_info(candidates);")


# %%
make_query("PRAGMA table_info(contributors);")


# %%
candidate_cols = [e[1] for e in make_query("PRAGMA table_info(candidates);")]
candidate_cols


# %%
contributor_cols = [e[1] for e in make_query("PRAGMA table_info(contributors);")]
contributor_cols


# %%
out=make_query("SELECT * FROM candidates;")
make_frame(out, legend=candidate_cols)

# %% [markdown]
# ## THE GRAMMAR OF DATA
# %% [markdown]
# Let us now focus on core data manipulation commands. The reason to do this is that they are *universal across systems, and by identifying them, we can quickly ask how to do these* when we encounter a new system.
# %% [markdown]
# See https://gist.github.com/TomAugspurger/6e052140eaa5fdb6e8c0/ which has a comparison of r/dplyr and pandas. I stole and modified this table from there:
# 
# ``dplyr`` has a small set of nicely defined verbs, which Hadley Wickham has used to identify core data manipulation commands. Here are listed the closest SQL and Pandas verbs, so we can see the universality of these manipulations.
# 
# 
# <table>
#   <tr>
#     <th><b>VERB</b></th>
#     <th><b>dplyr</b></th>
#     <th><b>pandas</b></th>
#     <th><b>SQL</b></th>
#   </tr>
#   <tr>
#     <td>QUERY/SELECTION</td>
#     <td>filter() (and slice())</td>
#     <td>query() (and loc[], iloc[])</td>
#     <td>SELECT WHERE</td>
#   </tr>
#   <tr>
#     <td>SORT</td>
#     <td>arrange()</td>
#     <td>sort_values()</td>
#     <td>ORDER BY</td>
#   </tr>
#   <tr>
#     <td>SELECT-COLUMNS/PROJECTION</td>
#     <td>select() (and rename())</td>
#     <td>[](__getitem__) (and rename())</td>
#     <td>SELECT COLUMN</td>
#   </tr>
#   <tr>
#     <td>SELECT-DISTINCT</td>
#     <td>distinct()</td>
#     <td>unique(),drop_duplicates()</td>
#     <td>SELECT DISTINCT COLUMN</td>
#   </tr>
#   <tr>
#     <td>ASSIGN</td>
#     <td>mutate() (and transmute())</td>
#     <td>assign</td>
#     <td>ALTER/UPDATE</td>
#   </tr>
#   <tr>
#     <td>AGGREGATE</td>
#     <td>summarise()</td>
#     <td>describe(), mean(), max()</td>
#     <td>None, AVG(),MAX()</td>
#   </tr>
#   <tr>
#     <td>SAMPLE</td>
#     <td>sample_n() and sample_frac()</td>
#     <td>sample()</td>
#     <td>implementation dep, use RAND()</td>
#   </tr>
#   <tr>
#     <td>GROUP-AGG</td>
#     <td>group_by/summarize</td>
#     <td>groupby/agg, count, mean</td>
#     <td>GROUP BY</td>
#   </tr>
#   <tr>
#     <td>DELETE</td>
#     <td>?</td>
#     <td>drop/masking</td>
#     <td>DELETE/WHERE</td>
#   </tr>
# </table>
# 
# %% [markdown]
# ### QUERY
# 
# We'll want to make queries on columns that are both numerical and categorical, combining these queries when appropriate...

# %%
dfcwci.query("20 <= amount <= 40")


# %%
out=make_query("SELECT * FROM contributors WHERE amount BETWEEN 20 AND 40;")
make_frame(out, legend=contributor_cols)

# %% [markdown]
# You can combine queries on different columns (here a numerical and a categorical):

# %%
dfcwci.query("state=='VA' & amount < 400")


# %%
dfcwci[(dfcwci.state=='VA') & (dfcwci.amount < 400)]


# %%
out=make_query("SELECT * FROM contributors WHERE state='VA' AND amount < 400;")
make_frame(out, legend=contributor_cols)

# %% [markdown]
# Looking for data problems with nulls:

# %%
dfcwci[dfcwci.state.isnull()]


# %%
out=make_query("SELECT * FROM contributors WHERE state IS NULL;")
make_frame(out, legend=contributor_cols)

# %% [markdown]
# Or the other way....

# %%
dfcwci[dfcwci.state.notnull()] # or dfcwci[~dfcwci.state.isnull()]


# %%
out=make_query("SELECT * FROM contributors WHERE state IS NOT NULL;")
make_frame(out, legend=contributor_cols)

# %% [markdown]
# Queries that look if things are in a list...useful for categorical variables or discrete valued numerical variables...

# %%
dfcwci[dfcwci.state.isin(['VA','WA'])].head(10)


# %%
out=make_query("SELECT * FROM contributors WHERE state IN ('VA','WA');")
make_frame(out, legend=contributor_cols).head(10)

# %% [markdown]
# ### SORT
# 
# After possibly making a query, we want to sort our results...

# %%
dfcwci.sort_values("amount").head(10)


# %%
dfcwci.sort_values("amount", ascending=False).head(10)


# %%
out=make_query("SELECT * FROM contributors ORDER BY amount;")
make_frame(out, legend=contributor_cols).head(10)


# %%
out=make_query("SELECT * FROM contributors ORDER BY amount DESC;")
make_frame(out, legend=contributor_cols).head(10)

# %% [markdown]
# ### SELECT-COLUMNS
# 
# Sometimes we only want to get a few columns, not the entire table

# %%
dfcwci[['first_name', 'amount']].head(10)


# %%
out=make_query("SELECT first_name, amount FROM contributors;")
make_frame(out,['first_name', 'amount']).head(10)

# %% [markdown]
# ### SELECT-DISTINCT
# 
# Once we have chosen certain columns we might want to drop rows which have duplicate values for some of these columns..
# 
# Selecting a distinct set is useful for cleaning. Here, we might wish to focus on contributors rather than contributions and see how many distinct contributors we have. Of-course we might be wrong, some people have identical names. 

# %%
dfcwci[['last_name','first_name']].count()


# %%
dfcwci[['last_name','first_name']].drop_duplicates().count()


# %%
out=make_query("SELECT DISTINCT last_name, first_name FROM contributors;")
make_frame(out,['last_name', 'first_name'])

# %% [markdown]
# ## Creation and Alteration
# %% [markdown]
# So far, when we created the database, we did it using Pandas. Clearly, one ought to be able to populate a SQL database using SQL. We now turn to this use case, as well as the alteration of databases.
# %% [markdown]
# ### Populate with SQL INSERT
# %% [markdown]
# Before we populate a the `candidates` table, lets delete all data from it...

# %%
rem="""
DELETE FROM candidates;
"""
c=db.cursor().execute(rem)
db.commit()

# %% [markdown]
# Once again, lets look at the structure of the candidates file.
# 
# Here is what the data looks like from the file `data/candidates.txt`
# 
# ```
# id|first_name|last_name|middle_name|party
# 33|Joseph|Biden||D
# 36|Samuel|Brownback||R
# 34|Hillary|Clinton|R.|D
# 39|Christopher|Dodd|J.|D
# 26|John|Edwards||D
# 22|Rudolph|Giuliani||R
# 24|Mike|Gravel||D
# 16|Mike|Huckabee||R
# 30|Duncan|Hunter||R
# 31|Dennis|Kucinich||D
# 37|John|McCain||R
# 20|Barack|Obama||D
# 32|Ron|Paul||R
# 29|Bill|Richardson||D
# 35|Mitt|Romney||R
# 38|Tom|Tancredo||R
# 41|Fred|Thompson|D.|R
# ```
# 
# We compose an insertion template using the SQL insertion command...

# %%
ins="""
INSERT INTO candidates (id, first_name, last_name, middle_name, party) \
    VALUES (?,?,?,?,?);
"""

# %% [markdown]
# Now we read the file line by line, not including the header line and slurp in the data. Notice that we only finish the transaction after we have slurped in all the lines. So its all lines or none. When we execute the cursor, the question marks are used a templates with a tuple provided in..

# %%
with open("data/candidates.txt") as fd:
    slines =[l.strip().split('|') for l in fd.readlines()]
    for line in slines[1:]:
        theid, first_name, last_name, middle_name, party = line
        print(theid, first_name, last_name, middle_name, party)
        valstoinsert = (int(theid), first_name, last_name, middle_name, party)
        print(ins, valstoinsert)
        db.cursor().execute(ins, valstoinsert)
        
db.commit()    


# %%
make_query("SELECT * FROM candidates;")

# %% [markdown]
# ### Create new columns with ASSIGN
# 
# In Pandas it is simple to create a new column (`pd.Series`) in the dataframe:

# %%
dfcwci['name']=dfcwci['last_name']+", "+dfcwci['first_name']
dfcwci.head(10)

# %% [markdown]
# One can also use `assign`, which creates a new dataframe...

# %%
newdf = dfcwci.assign(ucname=dfcwci.last_name+":"+dfcwci.first_name).head(10)
newdf.head()

# %% [markdown]
# Will the above command actually change `dfcwci`? `assign` creates a whole new dataframe. The dictionary style assigns in memory.
# 
# **What if we wanted to change an existing assignment?**
# 
# In other words, we are not creating a new column, but rather changing the values associated with a column, based on some criterion

# %%
dfcwci[dfcwci.state=='VA']

# %% [markdown]
# Lets get the `name` column we just created...

# %%
dfcwci.loc[dfcwci.state=='VA', 'name']

# %% [markdown]
# Now we set the names from Virginia to be "junk":

# %%
dfcwci.loc[dfcwci.state=='VA', 'name']="junk"


# %%
dfcwci.query("state=='VA'")['name']

# %% [markdown]
# ---
# 
# Let us see the entire process in SQL

# %%
alt="ALTER TABLE contributors ADD COLUMN name;"
db.cursor().execute(alt)
db.commit()


# %%
make_query("PRAGMA table_info(contributors);")


# %%
out = make_query("SELECT id, last_name,first_name from contributors;")
out2 = [(e[1]+", "+e[2],e[0]) for e in out]
out2[:5]


# %%
alt2="UPDATE contributors SET name = ? WHERE id = ?;"
for ele in out2:
    db.cursor().execute(alt2, ele)
db.commit()


# %%
out=make_query("SELECT * from contributors;")
print(out[0])
make_frame(out,legend=contributor_cols+["name"]).head(10)

# %% [markdown]
# And lets now do an assignment to an existing column

# %%
upd="UPDATE contributors SET name = 'junk' WHERE state = 'VA';"
db.cursor().execute(upd)
db.commit()


# %%
out=make_query("SELECT * from contributors where state='VA';")
make_frame(out,contributor_cols+["name"]).head(10)

# %% [markdown]
# ### No DROP COLUMN in SQLITE
# 
# Its available in other databases. Here you must just re-create your database, or know about this gotcha from the start. (reasons here: https://www.quora.com/Why-cant-SQLite-drop-columns)

# %%
alt="ALTER TABLE contributors DROP COLUMN name;"
db.cursor().execute(alt)
db.commit()

# %% [markdown]
# Its much simpler in Pandas, of-course

# %%
del dfcwci['name']

# %% [markdown]
# ### AGGREGATE

# %%
dfcwci.describe()


# %%
dfcwci.amount.max()


# %%
dfcwci[dfcwci.amount==dfcwci.amount.max()]

# %% [markdown]
# SQL is actually simpler

# %%
out=make_query("SELECT MAX(amount) FROM contributors;")
out


# %%
out=make_query("SELECT *, MAX(amount) AS maxamt FROM contributors;")
print(out)
make_frame(out, legend=contributor_cols+['name','maxamt'])


# %%
out=make_query("SELECT COUNT(amount) AS AMOUNTCOUNT FROM contributors;")
print(out)


# %%
dfcwci[dfcwci.amount > dfcwci.amount.max() - 2300]


# %%
out=make_query("SELECT * FROM contributors WHERE amount > (select (MAX(amount) - 2300) FROM contributors);")
make_frame(out, legend=contributor_cols)

# %% [markdown]
# Aso `MIN`, `SUM`, `AVG`.
# %% [markdown]
# ### GROUP-AGG
# 
# Being able to group data by the value of any column is critical functionality. This is how you understand your sampling, and the distributions of various quantities in your data.

# %%
dfcwci.groupby("state").sum()


# %%
dfcwci.groupby("state")['amount'].mean()


# %%
out=make_query("SELECT state,SUM(amount) FROM contributors GROUP BY state;")
make_frame(out, legend=['state','sum'])

# %% [markdown]
# ### DELETE

# %%
dfcwci.head()

# %% [markdown]
# In-place drops

# %%
df2=dfcwci.copy()
df2.set_index('last_name', inplace=True)
df2.head()


# %%
df2.drop(['Ahrens'], inplace=True)
df2.head()


# %%
df2.reset_index(inplace=True)
df2.head()

# %% [markdown]
# The recommended way to do it is to create a new dataframe. This might be impractical if data is very large.

# %%
dfcwci=dfcwci[dfcwci.last_name!='Ahrens']
dfcwci.head(10)


# %%
drow="DELETE FROM contributors WHERE last_name=\"Ahrens\";"
db.cursor().execute(drow)


# %%
db.commit()
out=make_query("SELECT * FROM contributors;")
make_frame(out, legend=contributor_cols).head(10)

# %% [markdown]
# ### LIMIT

# %%
out=make_query("SELECT * FROM contributors LIMIT 3;")
make_frame(out, legend=contributor_cols).head(10)


# %%
dfcwci[0:3]

# %% [markdown]
# ## Denormalization of Relationships: Two Table Grammar of Data
# 
# We will soon see that to feed data to models, we want to denormalize it. Pointers to other tables make logical sense for storage, but not when we want to feed data, both for performance and array shape reasons. This denormalization is achived by a technique and a verb: JOIN.
# 
# JOINs are Cartesian Products followed by filterings. They come in different varieties, and all pay attention to the "left" element in the join. The standard Pandas merge is an inner join, and often you will see it being done with 2 dataframes on a commonly named column.
# 
# Here the `candidate_id` column in the contributors table is equivalent to the `id` in the candidate table, so we need to be explicit:

# %%
dfcwci.shape, dfcand.shape


# %%
dfcwci.merge(dfcand, left_on="candidate_id", right_on="id")

# %% [markdown]
# This command repeats information about the candidate on each contributor to that candidate. Now you have a flat table.
# 
# If you do it in the opposite direction, the result is symmetric, since the `id` is guaranteed to match the `candidate_id` in out case

# %%
dfcand.merge(dfcwci, right_on="candidate_id", left_on="id")

# %% [markdown]
# ### Explicit INNER JOIN
# 
# The notion above (and the default) in Pandas is an inner join. Think of a cartesian product of the left table by the right one, 16 choices, followed by a drop of all the unmatched rows. Thus it gives us rows that are in both tables:
# 
# ![](images/innerjoin.png)
# 
# (The set images are from http://blog.codinghorror.com/a-visual-explanation-of-sql-joins/ which also has a very nice description of these joins).
# %% [markdown]
# ![inner join](http://pandas.pydata.org/pandas-docs/stable/_images/merging_merge_on_key_inner.png)
# 
# (from http://pandas.pydata.org/pandas-docs/stable/merging.html)

# %%
cols_wanted=['last_name_x', 'first_name_x', 'candidate_id', 'id', 'last_name_y']
dfcwci.merge(dfcand, left_on="candidate_id", right_on="id")[cols_wanted]


# %%
explicitjoinsel="""
SELECT 
    contributors.last_name, contributors.first_name, candidates.last_name 
FROM 
    contributors JOIN candidates 
ON contributors.candidate_id = candidates.id;
"""
out=make_query(explicitjoinsel)
make_frame(out, legend=["contributors.last_name", 
            "contributors.first_name",  "candidates.last_name"]).head()

# %% [markdown]
# Here is an usage example:

# %%
explicitjoinsel="""
SELECT 
    COUNT(contributors.id), contributors.first_name, candidates.last_name 
FROM 
    contributors JOIN candidates 
ON contributors.candidate_id = candidates.id

GROUP BY candidates.last_name;
"""
out=make_query(explicitjoinsel)
make_frame(out, legend=["count(contributors.id)", 
            "contributors.first_name",  "candidates.last_name"])

# %% [markdown]
# ### Outer JOIN
# %% [markdown]
# #### left outer (contributors on candidates)
# 
# This makes sure that everything from the first table is present. Where there is data in the second table corresponding to that in the first table it is preserved, but when there isnt a match in the right table, nulls are used.. 
# %% [markdown]
# ![](images/leftouterjoin.png)
# %% [markdown]
# ![left outer](http://pandas.pydata.org/pandas-docs/stable/_images/merging_merge_on_key_left.png)

# %%
dfcwci.merge(dfcand, left_on="candidate_id", right_on="id", how="left")[cols_wanted]


# %%
explicitjoinsel="""
SELECT 
    COUNT(contributors.id), contributors.first_name, candidates.last_name,
        contributors.candidate_id, candidates.id
FROM 
    contributors LEFT OUTER JOIN candidates 
ON contributors.candidate_id = candidates.id

GROUP BY candidates.last_name;
"""
out=make_query(explicitjoinsel)
make_frame(out, legend=["count(contributors.id)", "contributors.first_name",  
            "contributors.candidate_id", "candidates.id", "candidates.last_name"])

# %% [markdown]
# #### right outer (contributors on candidates)
# 
# 
# %% [markdown]
# ![right outer](http://pandas.pydata.org/pandas-docs/stable/_images/merging_merge_on_key_right.png)
# %% [markdown]
# This one guarantees that all the rows in the right one are present. The rows on the left if matched are there, else the corresponding columns are full of nulls

# %%
dfcwci.merge(dfcand, left_on="candidate_id", right_on="id", how="right")[cols_wanted]

# %% [markdown]
# Sqlite has no support for right outer or plain outer. If it did we could write:
# 
# ```sql
# SELECT 
#     COUNT(contributors.id), contributors.first_name, candidates.last_name 
# FROM 
#     contributors RIGHT OUTER JOIN candidates 
# ON contributors.candidate_id = candidates.id
# 
# GROUP BY candidates.last_name;
# ```
# %% [markdown]
# Instead we note that `right outer (contributors on candidates) = left outer (candidates on contributors)` and use that to make our join.

# %%
explicitjoinsel="""
SELECT 
    COUNT(contributors.id), contributors.first_name, candidates.last_name, 
        contributors.candidate_id, candidates.id
FROM 
    candidates LEFT OUTER JOIN contributors 
ON contributors.candidate_id = candidates.id

GROUP BY candidates.last_name;
"""
out=make_query(explicitjoinsel)
make_frame(out, legend=["count(contributors.id)", "contributors.first_name",  
                    "contributors.candidate_id", "candidates.id", "candidates.last_name"])

# %% [markdown]
# #### full outer
# 
# 
# ![](images/outerjoin.png)
# 
# Here matching records from both sides are available. Where the other side does not match, we put in nulls.

# %%
dfcwci.merge(dfcand, left_on="candidate_id", right_on="id", how="outer")[cols_wanted]

# %% [markdown]
# ![outer](http://pandas.pydata.org/pandas-docs/stable/_images/merging_merge_on_key_outer.png)
# %% [markdown]
# also not supported by sqlite
# 
# ```sql
# SELECT 
#     COUNT(contributors.id), contributors.first_name, candidates.last_name 
# FROM 
#     contributors FULL OUTER JOIN candidates 
# ON contributors.candidate_id = candidates.id
# 
# GROUP BY candidates.last_name;
# ```
# %% [markdown]
# When to use which?
# 
# See this:
# 
# http://blog.codinghorror.com/a-visual-explanation-of-sql-joins/
# %% [markdown]
# ## Some Other Useful patterns
# %% [markdown]
# ### Simple subselect
# 
# We do one "select" and then use that in another one...

# %%
dfcand.head()


# %%
obamaid=dfcand.query("last_name=='Obama'")['id'].values[0]


# %%
obamacontrib=dfcwci.query("candidate_id==%i" % obamaid)
obamacontrib.head()

# %% [markdown]
# The SQL syntax makes this look like a russian doll scenario where there is a select inside a select..

# %%
russiandollsel="""
SELECT * FROM contributors WHERE 
    candidate_id = (SELECT id from candidates WHERE last_name = 'Obama');
"""
out=make_query(russiandollsel)
make_frame(out, legend=contributor_cols)

# %% [markdown]
# ### Implicit join
# 
# This is a SQL only construct which is nevertheless an often seen pattern where we dont do an explicit inner join...

# %%
implicitjoinsel="""
SELECT 
    contributors.last_name, contributors.first_name, contributors.amount, candidates.last_name 
FROM 
    contributors, candidates 
WHERE contributors.candidate_id = candidates.id
AND candidates.last_name = 'Obama';
"""
out=make_query(implicitjoinsel)
make_frame(out, legend=["contributors.last_name", 
            "contributors.first_name", "contributors.amount", "candidates.last_name"]).head()

# %% [markdown]
# Let's expand to not just include Obama, so it looks more like a regular inner join

# %%
implicitjoinsel="""
SELECT 
    contributors.last_name, contributors.first_name, contributors.amount, candidates.last_name 
FROM 
    contributors, candidates 
WHERE contributors.candidate_id = candidates.id;
"""
out=make_query(implicitjoinsel)
make_frame(out, legend=["contributors.last_name", 
            "contributors.first_name", "contributors.amount", "candidates.last_name"]).head()

# %% [markdown]
# ## Pandas /SQL
# 
# Lastly, Pandas can execute SQL for you once you have a DBAPI2 connection! So you dont need the support code we wroe to display dataframes from SQL output.

# %%
pd.read_sql("SELECT * FROM candidates WHERE party= 'D';", db)


# %%
pd.read_sql(implicitjoinsel, db)

# %% [markdown]
# This is very useful if the database is big and out of memory. Sqlite3 is the only db2api database supported. For any other database you should use `SQLAlchemy`. See, for eg: https://plot.ly/ipython-notebooks/big-data-analytics-with-pandas-and-sqlite/

# %%
db.close()


