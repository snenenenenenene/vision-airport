# Toegepaste Informatica

## Analyse Vision Airport

Onderdeel van Integrated Project Big Data
ondersteund door de
_Artesis Plantijn Hogeschool_
**Senne Bels, Lenny Bontenakel, Youssef El Boujeddainim & Prem Kokra**

---

### Opdrachtgever

De opdrachtgever van dit project is Ordina. Ordina is een Nederlands bedrijf dat over de hele Benelux kantoren heeft. Ze focussen zich voornamelijk op het bouwen en beheren van oplossingen voor de automatisering van bedrijfsprocessen en ICT.

Bij Ordina hebben we verschillende contactpersonen die elk hun kennis hebben in een ander onderwerp. We hebben contact gehad met **Aron Geerts**.

Dries Van Hansewijck, lector van AP Hogeschool, zal het project opvolgen en het nodige platform voorzien.

### Samenvatting

VisionAirport is al jaren verwikkeld in een steekspel op zoek naar de maximale groei. De vraag naar vluchten neemt elk jaar weer toe, ondanks enkele tegenslagen die de luchtvaartsector de laatste jaren heeft gehad zoals COVID. Steeds meer factoren krijgen invloed op de toekomst van VisionAirport. De internationale verhoudingen veranderen en zaken als geluidshinder en milieuvervuiling spelen een steeds grotere rol.

### Situatie As-Is

#### Probleemstelling

Vision Airport is sinds enkele jaren een commerciële luchthaven, ze moeten dus verantwoording afleggen bij stakeholders, overheid en media. Deze informatie moet ook steeds meer en meer in detail zijn. Het verzamelen, groeperen en het rapporteren van de data is de verantwoordelijkheid van de afdeling “informatiemanagement”. Ze hebben op verschillende plaatsen in de organisatie databronnen. Hierdoor is het verzamelen en standaardiseren van de data een tijdrovend proces. Pas daarna kunnen ze beginnen aan het maken van rapporten en dashboards, waardoor ze maar zeer laat bij directie stakeholders en media terecht komen. Dit heeft invloed op de snelheid en accuraatheid waarmee men beslissingen kan maken.

### Situatie To-Be

#### Doelstelling

Dit project gaat ervoor zorgen dat we de data centraliseren, uit de flat files halen en standardiseren waardoor er gelijk rapporten gemaakt kunnen worden. Om de snelheid van het maken van beslissing te acceleren zullen we naast het gebruik van een **data lake** om de rapporteringen te automatiseren ook nog een **Machine Learning toepassing** toevoegen aan het project.

#### Scope

- BI-platform
- Dashboard
- Rapportering
- Ingestion
- Cleaning
- Exploration
- Technische Documentatie
- Datalake
- AI-model

#### Niet in Scope

- Vooronderzoek
- Afbakening
- Architectuur- en infrastructuur analyse
- Infrastructuur implementatie
- Organisatorische implementatie
- Full-client
- Sofwareselectietraject

### Planning

| Hoofdlijn    | Deadline |
| ------------ | -------- |
| Sprint 1 |  |
| Analyse      | 14/12/21 |
| Notebook     | 14/12/21 |
| Sprint 2 |  |
| Rapportering | 21/12/21 |
| ML-model     | 21/12/21 |

#### Toelichting Fases

1. **Analyse**:
We maken een blueprint waarin we een analyse maken van het project. De analyse moet een duidelijker beeld scheppen van het project.

2. **Design**:
We creëren een Proof of Concept (POC) waarop we in de volgende fase verder op kunnen bouwen.
Deze POC zal ook een duidelijke weergave voorbrengen in verband met wat we zullen opleveren en hoe we dit willen realiseren.

3. **Construct**:
We ontwikkelen een dashboard en rapporteringen met AWS Quicksight alsook een ML-model dat voorspellingen zal doen.

 1. **Turnover**:
Hier presenteren we onze bevindingen aan Ordina en lector Dries Van Hansewijck.

### Technisch Design
#### Microservices
<img src="./assets/aws.png" alt="drawing" width="100"/>

**AWS S3**
**AWS Athena**
**AWS Quicksight**
#### Machine Learning
<img src="./assets/python.png" alt="drawing" width="100"/>

**Python**
**Jupyter**
**Matplotlib**
**Pyspark**
**Tensorflow**
#### Teksteditor
<img src="./assets/vscode.png" alt="drawing" width="100"/>

**VSCode**

### Functioneel Design

### Impact op huidige infrastructuur
