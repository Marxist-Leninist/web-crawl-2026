#!/usr/bin/env python3
"""Generate ~10M+ seed URLs from multiple sources for the web crawler."""
import random, os, sys, hashlib

OUTPUT = "/workspace/mega_seeds_v2.txt"
urls = set()

def log(msg):
    print(f"[seed-gen] {msg}", flush=True)

# ========== 1. Top domains with deep path generation ==========
TOP_DOMAINS = [
    # News & Media (60+)
    "bbc.com", "reuters.com", "theguardian.com", "nytimes.com", "washingtonpost.com",
    "cnn.com", "aljazeera.com", "apnews.com", "npr.org", "pbs.org",
    "theatlantic.com", "newyorker.com", "economist.com", "ft.com", "bloomberg.com",
    "wired.com", "arstechnica.com", "theverge.com", "techcrunch.com", "engadget.com",
    "vice.com", "slate.com", "salon.com", "vox.com", "politico.com",
    "bbc.co.uk", "independent.co.uk", "telegraph.co.uk", "sky.com",
    "abc.net.au", "smh.com.au", "stuff.co.nz",
    "cbc.ca", "globeandmail.com",
    "spiegel.de", "zeit.de", "lemonde.fr", "elpais.com", "corriere.it",
    "straitstimes.com", "scmp.com", "japantimes.co.jp", "hindustantimes.com",
    "timesofindia.indiatimes.com", "ndtv.com", "thehindu.com",
    "dawn.com", "bangkokpost.com", "koreaherald.com",
    "dw.com", "france24.com", "euronews.com", "rfi.fr",
    "channelnewsasia.com", "rt.com", "tass.com",
    "latimes.com", "chicagotribune.com", "bostonglobe.com", "sfgate.com",
    "seattletimes.com", "denverpost.com", "dallasnews.com", "miamiherald.com",
    "usatoday.com", "foxnews.com", "nbcnews.com", "cbsnews.com", "abcnews.go.com",
    "newsweek.com", "time.com", "usnews.com", "thehill.com", "axios.com",
    "propublica.org", "theintercept.com", "motherjones.com", "thedailybeast.com",
    "rawstory.com", "huffpost.com", "buzzfeednews.com",
    # Science & Education (40+)
    "nature.com", "sciencedirect.com", "plos.org", "arxiv.org",
    "ncbi.nlm.nih.gov", "pubmed.ncbi.nlm.nih.gov", "nih.gov",
    "nasa.gov", "esa.int", "cern.ch",
    "britannica.com", "newscientist.com", "scientificamerican.com",
    "smithsonianmag.com", "nationalgeographic.com",
    "khanacademy.org", "coursera.org", "edx.org",
    "sciencemag.org", "cell.com", "thelancet.com", "bmj.com",
    "physicstoday.org", "aps.org", "acs.org", "rsc.org",
    "ieee.org", "acm.org", "springer.com", "wiley.com", "elsevier.com",
    "jstor.org", "researchgate.net", "academia.edu",
    "livescience.com", "sciencenews.org", "phys.org", "sciencedaily.com",
    "popularmechanics.com", "popularsciencemag.com", "discovermagazine.com",
    # Tech & Dev (40+)
    "github.com", "stackoverflow.com", "dev.to", "medium.com",
    "hackernoon.com", "lobste.rs", "slashdot.org",
    "docs.python.org", "developer.mozilla.org",
    "w3schools.com", "geeksforgeeks.org", "tutorialspoint.com",
    "realpython.com", "freecodecamp.org", "digitalocean.com",
    "linux.die.net", "man7.org", "kernel.org",
    "infoq.com", "dzone.com", "baeldung.com", "vogella.com",
    "tldp.org", "linuxjournal.com", "lwn.net",
    "css-tricks.com", "smashingmagazine.com", "alistapart.com",
    "martinfowler.com", "joelonsoftware.com", "paulgraham.com",
    "blog.codinghorror.com", "scottberkun.com",
    "towardsdatascience.com", "kdnuggets.com", "analyticsvidhya.com",
    "machinelearningmastery.com", "distill.pub", "openai.com",
    "huggingface.co", "paperswithcode.com",
    # Reference & Knowledge (20+)
    "en.wikipedia.org", "en.wiktionary.org", "en.wikisource.org",
    "en.wikiquote.org", "en.wikibooks.org", "commons.wikimedia.org",
    "simple.wikipedia.org",
    "archive.org", "gutenberg.org", "openlibrary.org",
    "plato.stanford.edu", "iep.utm.edu",
    "snopes.com", "factcheck.org", "politifact.com",
    "dictionary.com", "merriam-webster.com", "etymonline.com",
    "howstuffworks.com", "explainthatstuff.com",
    # Government (40+)
    "cdc.gov", "nih.gov", "fda.gov", "epa.gov", "nasa.gov", "noaa.gov",
    "usgs.gov", "energy.gov", "education.gov", "treasury.gov", "state.gov",
    "nist.gov", "nsf.gov", "doi.gov", "usda.gov",
    "loc.gov", "archives.gov", "bls.gov", "census.gov",
    "weather.gov", "nps.gov", "fcc.gov", "ftc.gov", "sec.gov",
    "gov.uk", "nhs.uk", "parliament.uk", "ons.gov.uk",
    "canada.ca", "abs.gov.au", "stats.govt.nz",
    "europa.eu", "who.int", "un.org", "worldbank.org", "imf.org",
    "oecd.org", "wto.org", "iaea.org", "unesco.org",
    # Culture & Arts (20+)
    "imdb.com", "rottentomatoes.com", "metacritic.com",
    "allmusic.com", "discogs.com", "genius.com",
    "goodreads.com", "librarything.com",
    "moma.org", "metmuseum.org", "britishmuseum.org",
    "artnet.com", "artsy.net", "christies.com", "sothebys.com",
    # Health (15+)
    "webmd.com", "mayoclinic.org", "healthline.com",
    "medlineplus.gov", "drugs.com", "medscape.com",
    "clevelandclinic.org", "hopkinsmedicine.org", "mountsinai.org",
    "patient.info", "nhs.uk",
    # Business & Finance (15+)
    "investopedia.com", "fool.com", "seekingalpha.com",
    "marketwatch.com", "cnbc.com", "wsj.com",
    "forbes.com", "businessinsider.com", "inc.com", "fastcompany.com",
    "hbr.org", "mckinsey.com", "bcg.com",
    # Food & Lifestyle (10+)
    "allrecipes.com", "foodnetwork.com", "epicurious.com",
    "seriouseats.com", "bonappetit.com", "cookinglight.com",
    "food52.com", "tasteatlas.com",
    # Travel & Geography (10+)
    "lonelyplanet.com", "tripadvisor.com", "atlasobscura.com",
    "worldatlas.com", "roughguides.com",
]

# 200 topic words for path generation
WORDS = [
    "climate-change", "artificial-intelligence", "quantum-computing", "renewable-energy",
    "machine-learning", "biodiversity", "cybersecurity", "blockchain", "nanotechnology",
    "gene-therapy", "space-exploration", "ocean-conservation", "nuclear-fusion",
    "autonomous-vehicles", "brain-computer-interface", "crispr", "dark-matter",
    "electric-vehicles", "food-security", "global-health", "hydrogen-energy",
    "internet-of-things", "lithium-batteries", "mars-colonization", "neural-networks",
    "organic-chemistry", "particle-physics", "quantum-entanglement", "robotics",
    "semiconductor", "vaccine-development", "water-purification",
    "ancient-civilizations", "medieval-history", "industrial-revolution",
    "world-war-two", "cold-war", "roman-empire", "byzantine-empire",
    "ottoman-empire", "mongol-empire", "silk-road", "renaissance",
    "enlightenment", "french-revolution", "american-revolution",
    "civil-rights-movement", "decolonization", "globalization",
    "democracy", "economics", "philosophy", "psychology", "sociology",
    "anthropology", "archaeology", "linguistics", "mathematics",
    "statistics", "calculus", "algebra", "geometry", "topology",
    "biology", "chemistry", "physics", "astronomy", "geology",
    "ecology", "genetics", "evolution", "immunology", "virology",
    "neuroscience", "pharmacology", "epidemiology", "nutrition",
    "architecture", "engineering", "materials-science", "aeronautics",
    "computer-science", "data-science", "cryptography", "algorithms",
    "operating-systems", "databases", "networking", "compilers",
    "distributed-systems", "cloud-computing", "microservices",
    "python-programming", "javascript", "rust-language", "golang",
    "music-theory", "film-history", "literature", "poetry",
    "painting", "sculpture", "photography", "theater", "dance",
    "cooking-techniques", "fermentation", "agriculture", "forestry",
    "marine-biology", "volcanology", "seismology", "meteorology",
    "climate-science", "paleontology", "entomology", "botany",
    "zoology", "mycology", "microbiology", "biochemistry",
    "organic-synthesis", "polymer-science", "thermodynamics",
    "fluid-dynamics", "optics", "acoustics", "electromagnetism",
    "superconductivity", "plasma-physics", "astrophysics",
    "cosmology", "general-relativity", "string-theory",
    "cognitive-science", "behavioral-economics", "game-theory",
    "information-theory", "chaos-theory", "complexity-theory",
    "number-theory", "graph-theory", "combinatorics",
    "differential-equations", "linear-algebra", "probability",
    "machine-vision", "natural-language-processing", "reinforcement-learning",
    "transformer-models", "generative-ai", "computer-graphics",
    "virtual-reality", "augmented-reality", "human-computer-interaction",
    "software-engineering", "devops", "agile-methodology",
    "design-patterns", "functional-programming", "deep-learning",
    "attention-mechanisms", "diffusion-models", "genetic-algorithms",
    "cellular-automata", "swarm-intelligence", "sustainability",
    "urban-planning", "public-health", "mental-health", "education-reform",
    "immigration", "trade-policy", "fiscal-policy", "monetary-policy",
    "supply-chain", "logistics", "manufacturing", "automation",
    "3d-printing", "biotechnology", "synthetic-biology", "bioinformatics",
    "proteomics", "genomics", "transcriptomics", "metabolomics",
    "spectroscopy", "chromatography", "crystallography", "microscopy",
    "signal-processing", "control-theory", "optimization",
    "stochastic-processes", "bayesian-inference", "regression-analysis",
    "time-series", "dimensionality-reduction", "clustering",
    "anomaly-detection", "recommender-systems", "knowledge-graphs",
    "federated-learning", "transfer-learning", "few-shot-learning",
    "self-supervised-learning", "contrastive-learning", "meta-learning",
    "multi-agent-systems", "evolutionary-computation", "fuzzy-logic",
    "formal-verification", "type-theory", "category-theory",
    "abstract-algebra", "real-analysis", "complex-analysis",
    "measure-theory", "functional-analysis", "harmonic-analysis",
]

PATH_PATTERNS = [
    "/wiki/{word}", "/article/{word}", "/news/{word}", "/topic/{word}",
    "/category/{word}", "/tag/{word}", "/blog/{word}", "/posts/{word}",
    "/science/{word}", "/technology/{word}", "/health/{word}",
    "/politics/{word}", "/business/{word}", "/culture/{word}",
    "/world/{word}", "/opinion/{word}", "/analysis/{word}",
    "/features/{word}", "/reviews/{word}", "/guides/{word}",
    "/how-to/{word}", "/tutorial/{word}", "/learn/{word}",
    "/research/{word}", "/papers/{word}", "/docs/{word}",
    "/history/{word}", "/environment/{word}", "/education/{word}",
    "/en/{word}", "/en/article/{word}", "/en/news/{word}",
    "/topics/{word}", "/sections/{word}", "/archives/{word}",
    "/explore/{word}", "/discover/{word}", "/search?q={word}",
    "/{word}", "/stories/{word}", "/insights/{word}",
]

log("Phase 1: Domain + path URLs...")
for domain in TOP_DOMAINS:
    urls.add(f"https://{domain}/")
    urls.add(f"https://www.{domain}/")
    for pattern in PATH_PATTERNS:
        for word in WORDS:
            url = f"https://{domain}{pattern}".replace("{word}", word)
            urls.add(url)
            # Also with www
            url2 = f"https://www.{domain}{pattern}".replace("{word}", word)
            urls.add(url2)
    # Date-based archives
    for year in range(2020, 2027):
        for month in range(1, 13):
            urls.add(f"https://{domain}/{year}/{month:02d}/")
            urls.add(f"https://www.{domain}/{year}/{month:02d}/")
            for word in random.sample(WORDS, 5):
                urls.add(f"https://{domain}/{year}/{month:02d}/{word}")

log(f"  Domain+path: {len(urls):,} URLs")

# ========== 2. Wikipedia — massive generation ==========
log("Phase 2: Wikipedia URLs...")

# Prefix + subject combinations
WIKI_PREFIXES = [
    "History_of_", "List_of_", "Geography_of_", "Economy_of_", "Culture_of_",
    "Demographics_of_", "Politics_of_", "Education_in_", "Religion_in_",
    "Music_of_", "Sport_in_", "Climate_of_", "Cuisine_of_", "Transport_in_",
    "Health_in_", "Science_and_technology_in_", "Tourism_in_", "Military_of_",
    "Architecture_of_", "Literature_of_", "Timeline_of_", "Outline_of_",
    "Index_of_", "Flag_of_", "Coat_of_arms_of_", "Capital_of_",
    "Languages_of_", "Ethnic_groups_in_", "Biodiversity_of_",
    "National_symbols_of_", "Human_rights_in_", "Taxation_in_",
    "Energy_policy_of_", "Foreign_relations_of_", "Law_of_",
    "Media_of_", "Cinema_of_", "Theatre_in_", "Dance_in_",
]

# Countries, cities, regions
PLACES = [
    "the_United_States", "the_United_Kingdom", "France", "Germany", "Japan",
    "China", "India", "Brazil", "Russia", "Canada", "Australia", "Mexico",
    "Italy", "Spain", "South_Korea", "Netherlands", "Sweden", "Norway",
    "Switzerland", "Poland", "Turkey", "Egypt", "South_Africa", "Nigeria",
    "Kenya", "Argentina", "Chile", "Colombia", "Peru", "Indonesia",
    "Thailand", "Vietnam", "Malaysia", "Philippines", "Pakistan",
    "Bangladesh", "Iran", "Iraq", "Saudi_Arabia", "Israel",
    "New_Zealand", "Ireland", "Scotland", "Wales", "Portugal", "Greece",
    "Czech_Republic", "Hungary", "Romania", "Bulgaria", "Croatia",
    "Serbia", "Ukraine", "Belarus", "Lithuania", "Latvia", "Estonia",
    "Finland", "Denmark", "Iceland", "Luxembourg", "Belgium",
    "Austria", "Slovakia", "Slovenia", "North_Macedonia", "Albania",
    "Montenegro", "Bosnia_and_Herzegovina", "Moldova", "Georgia_(country)",
    "Armenia", "Azerbaijan", "Kazakhstan", "Uzbekistan", "Turkmenistan",
    "Kyrgyzstan", "Tajikistan", "Mongolia", "Nepal", "Sri_Lanka",
    "Myanmar", "Cambodia", "Laos", "Brunei", "East_Timor",
    "Papua_New_Guinea", "Fiji", "Samoa", "Tonga",
    "Morocco", "Algeria", "Tunisia", "Libya", "Sudan", "Ethiopia",
    "Somalia", "Tanzania", "Uganda", "Rwanda", "Burundi",
    "Democratic_Republic_of_the_Congo", "Republic_of_the_Congo",
    "Cameroon", "Ghana", "Senegal", "Mali", "Niger",
    "Burkina_Faso", "Ivory_Coast", "Guinea", "Sierra_Leone",
    "Liberia", "Togo", "Benin", "Mauritania", "Madagascar",
    "Mozambique", "Zimbabwe", "Zambia", "Malawi", "Botswana",
    "Namibia", "Angola", "Gabon", "Equatorial_Guinea",
    "Cuba", "Haiti", "Dominican_Republic", "Jamaica", "Trinidad_and_Tobago",
    "Panama", "Costa_Rica", "Honduras", "Guatemala", "El_Salvador",
    "Nicaragua", "Belize", "Paraguay", "Uruguay", "Ecuador",
    "Bolivia", "Venezuela", "Guyana", "Suriname",
    # Cities
    "New_York_City", "London", "Paris", "Tokyo", "Berlin", "Rome",
    "Moscow", "Beijing", "Mumbai", "Sydney", "Toronto", "Dubai",
    "Singapore", "Hong_Kong", "Seoul", "Istanbul", "Cairo",
    "Los_Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San_Antonio", "San_Diego", "Dallas", "San_Jose", "Austin",
    "San_Francisco", "Seattle", "Denver", "Boston", "Nashville",
    "Portland", "Las_Vegas", "Miami", "Atlanta", "Minneapolis",
    "Detroit", "Pittsburgh", "Cleveland", "Cincinnati", "Milwaukee",
    "Bangkok", "Jakarta", "Manila", "Ho_Chi_Minh_City", "Kuala_Lumpur",
    "Shanghai", "Shenzhen", "Guangzhou", "Chengdu", "Wuhan",
    "Osaka", "Kyoto", "Nagoya", "Yokohama", "Sapporo",
    "Delhi", "Kolkata", "Chennai", "Bangalore", "Hyderabad",
    "Karachi", "Lahore", "Dhaka", "Colombo", "Kathmandu",
    "Lagos", "Nairobi", "Addis_Ababa", "Dar_es_Salaam", "Johannesburg",
    "Cape_Town", "Casablanca", "Algiers", "Tunis", "Accra",
    "Buenos_Aires", "Sao_Paulo", "Rio_de_Janeiro", "Lima", "Bogota",
    "Santiago", "Quito", "Caracas", "Montevideo", "La_Paz",
    "Mexico_City", "Havana", "Kingston", "Panama_City", "San_Juan",
    "Dublin", "Edinburgh", "Manchester", "Birmingham", "Glasgow",
    "Amsterdam", "Rotterdam", "Brussels", "Antwerp", "Zurich",
    "Geneva", "Vienna", "Prague", "Budapest", "Warsaw",
    "Krakow", "Bucharest", "Sofia", "Athens", "Lisbon",
    "Porto", "Barcelona", "Madrid", "Valencia", "Seville",
    "Milan", "Naples", "Florence", "Venice", "Turin",
    "Munich", "Hamburg", "Frankfurt", "Cologne", "Stuttgart",
    "Copenhagen", "Stockholm", "Oslo", "Helsinki", "Reykjavik",
    "St._Petersburg", "Kyiv", "Minsk", "Tbilisi", "Baku",
]

for prefix in WIKI_PREFIXES:
    for place in PLACES:
        urls.add(f"https://en.wikipedia.org/wiki/{prefix}{place}")

# Standalone articles — big list
WIKI_ARTICLES = [
    "Photosynthesis", "DNA", "RNA", "Protein", "Enzyme", "Cell_(biology)",
    "Mitochondrion", "Chloroplast", "Ribosome", "Chromosome", "Gene",
    "Evolution", "Natural_selection", "Speciation", "Taxonomy_(biology)",
    "Ecosystem", "Biome", "Food_web", "Biodiversity", "Conservation_biology",
    "Plate_tectonics", "Earthquake", "Volcano", "Tsunami", "Continental_drift",
    "Electron", "Proton", "Neutron", "Quark", "Photon", "Neutrino",
    "Higgs_boson", "Standard_Model", "Quantum_mechanics", "General_relativity",
    "Electromagnetism", "Thermodynamics", "Entropy", "Wave", "Frequency",
    "Periodic_table", "Chemical_element", "Chemical_compound", "Chemical_reaction",
    "Algorithm", "Data_structure", "Turing_machine", "P_versus_NP_problem",
    "Artificial_intelligence", "Machine_learning", "Neural_network",
    "Operating_system", "Computer_network", "Database", "Compiler",
    "Internet", "World_Wide_Web", "HTTP", "Encryption", "Blockchain",
    "Solar_System", "Sun", "Mercury_(planet)", "Venus", "Earth", "Mars",
    "Jupiter", "Saturn", "Uranus", "Neptune", "Moon",
    "Galaxy", "Milky_Way", "Black_hole", "Neutron_star", "Supernova",
    "Dark_matter", "Dark_energy", "Big_Bang", "Cosmic_microwave_background",
    "Calculus", "Linear_algebra", "Number_theory", "Set_theory",
    "Group_theory", "Topology", "Pi", "Prime_number", "Fibonacci_number",
    "Python_(programming_language)", "JavaScript", "Java_(programming_language)",
    "C_(programming_language)", "Rust_(programming_language)", "Go_(programming_language)",
    "Linux", "Unix", "Automobile", "Airplane", "Steam_engine",
    "Electric_motor", "Battery_(electricity)", "Solar_cell", "Wind_turbine",
    "Nuclear_power", "Semiconductor", "Transistor", "Integrated_circuit",
    "Printing_press", "Telescope", "Microscope", "Compass", "Clock",
    "Alexander_the_Great", "Julius_Caesar", "Genghis_Khan", "Napoleon",
    "Leonardo_da_Vinci", "Isaac_Newton", "Albert_Einstein", "Charles_Darwin",
    "Marie_Curie", "Nikola_Tesla", "Alan_Turing", "William_Shakespeare",
    "Democracy", "Capitalism", "Socialism", "United_Nations",
    "Olympic_Games", "FIFA_World_Cup", "Association_football",
    "Human_brain", "Heart", "Immune_system", "Vaccine", "Antibiotic",
    "Cancer", "Diabetes", "Malaria", "HIV/AIDS",
    "Agriculture", "Rice", "Wheat", "Coffee", "Tea",
    "Steel", "Iron", "Gold", "Concrete", "Plastic",
    "Bridge", "Dam", "Pyramid", "Great_Wall_of_China",
    "Oxygen", "Hydrogen", "Carbon", "Nitrogen", "Helium", "Lithium",
    "Sodium", "Potassium", "Calcium", "Magnesium", "Phosphorus", "Sulfur",
    "Chlorine", "Argon", "Silicon", "Aluminum", "Copper", "Zinc",
    "Tin", "Lead", "Mercury_(element)", "Uranium", "Plutonium",
    "Water", "Ammonia", "Methane", "Ethanol", "Glucose", "Cellulose",
    "Starch", "Lignin", "Keratin", "Collagen", "Hemoglobin", "Insulin",
    "Dopamine", "Serotonin", "Adrenaline", "Cortisol", "Testosterone",
    "Estrogen", "Melatonin", "Oxytocin", "Endorphin",
    "Bacteria", "Virus", "Fungus", "Archaea", "Protozoa",
    "Antibiotic_resistance", "Prion", "Bacteriophage", "Plasmid",
    "CRISPR", "Polymerase_chain_reaction", "Gel_electrophoresis",
    "Mass_spectrometry", "X-ray_crystallography", "Nuclear_magnetic_resonance",
    "Electron_microscope", "Scanning_tunneling_microscope",
    "Hubble_Space_Telescope", "James_Webb_Space_Telescope",
    "Large_Hadron_Collider", "ITER", "International_Space_Station",
    "Apollo_program", "Space_Shuttle", "SpaceX", "Mars_rover",
    "Voyager_program", "Cassini-Huygens", "New_Horizons",
    "Global_Positioning_System", "Satellite", "Fiber_optics",
    "5G", "Wi-Fi", "Bluetooth", "RFID", "Barcode",
    "Laser", "LED", "OLED", "Liquid_crystal_display",
    "Cathode-ray_tube", "Vacuum_tube", "Diode", "Capacitor",
    "Resistor", "Inductor", "Transformer", "Relay",
    "Electric_generator", "Electric_power_transmission",
    "Alternating_current", "Direct_current", "Superconductor",
    "Magnet", "Electromagnet", "Magnetic_resonance_imaging",
    "Computed_tomography", "Ultrasound", "Positron_emission_tomography",
    "Radioactivity", "Nuclear_fission", "Nuclear_fusion", "Isotope",
    "Carbon_dating", "Geiger_counter", "Dosimeter",
    "Renewable_energy", "Fossil_fuel", "Coal", "Petroleum", "Natural_gas",
    "Geothermal_energy", "Tidal_power", "Wave_power", "Biomass",
    "Photovoltaics", "Concentrated_solar_power", "Wind_farm",
    "Hydroelectricity", "Pumped-storage_hydroelectricity",
    "Climate_change", "Greenhouse_gas", "Carbon_dioxide", "Methane",
    "Ozone_layer", "Acid_rain", "Air_pollution", "Water_pollution",
    "Deforestation", "Desertification", "Coral_bleaching",
    "Mass_extinction", "Endangered_species", "Conservation",
    "National_park", "World_Heritage_Site", "Biosphere_reserve",
    "Amazon_rainforest", "Great_Barrier_Reef", "Sahara",
    "Arctic", "Antarctic", "Mariana_Trench", "Mount_Everest",
    "Grand_Canyon", "Niagara_Falls", "Victoria_Falls",
    "Nile", "Amazon_River", "Mississippi_River", "Yangtze",
    "Ganges", "Danube", "Rhine", "Thames", "Seine",
    "Pacific_Ocean", "Atlantic_Ocean", "Indian_Ocean", "Arctic_Ocean",
    "Mediterranean_Sea", "Caribbean_Sea", "South_China_Sea",
    "Pangaea", "Gondwana", "Laurasia", "Rodinia",
    "Cambrian_explosion", "Permian-Triassic_extinction_event",
    "Cretaceous-Paleogene_extinction_event", "Dinosaur",
    "Tyrannosaurus", "Triceratops", "Velociraptor", "Brachiosaurus",
    "Mammoth", "Saber-toothed_cat", "Megalodon",
    "Homo_sapiens", "Homo_erectus", "Neanderthal", "Australopithecus",
    "Stone_Age", "Bronze_Age", "Iron_Age",
    "Mesopotamia", "Ancient_Egypt", "Indus_Valley_Civilisation",
    "Ancient_Greece", "Ancient_Rome", "Han_dynasty", "Tang_dynasty",
    "Ming_dynasty", "Qing_dynasty", "Mughal_Empire", "Gupta_Empire",
    "Maurya_Empire", "Achaemenid_Empire", "Sassanid_Empire",
    "Maya_civilization", "Aztec_Empire", "Inca_Empire",
    "Viking_Age", "Crusades", "Black_Death", "Hundred_Years_War",
    "Age_of_Discovery", "Columbian_exchange", "Atlantic_slave_trade",
    "Scientific_Revolution", "Protestant_Reformation",
    "Thirty_Years_War", "Seven_Years_War", "Napoleonic_Wars",
    "Congress_of_Vienna", "Scramble_for_Africa", "Meiji_Restoration",
    "World_War_I", "Russian_Revolution", "Great_Depression",
    "World_War_II", "Holocaust", "Cold_War", "Korean_War",
    "Vietnam_War", "Space_Race", "Cuban_Missile_Crisis",
    "Berlin_Wall", "Dissolution_of_the_Soviet_Union",
    "September_11_attacks", "War_on_terror", "Iraq_War",
    "Arab_Spring", "COVID-19_pandemic",
    "Plato", "Aristotle", "Socrates", "Confucius", "Laozi",
    "Buddha", "Muhammad", "Jesus", "Moses", "Abraham",
    "Immanuel_Kant", "Friedrich_Nietzsche", "Karl_Marx",
    "John_Locke", "Thomas_Hobbes", "Jean-Jacques_Rousseau",
    "Voltaire", "David_Hume", "Rene_Descartes", "Baruch_Spinoza",
    "Georg_Wilhelm_Friedrich_Hegel", "Arthur_Schopenhauer",
    "Soren_Kierkegaard", "Ludwig_Wittgenstein", "Bertrand_Russell",
    "Noam_Chomsky", "Michel_Foucault", "Jacques_Derrida",
    "Simone_de_Beauvoir", "Hannah_Arendt", "John_Rawls",
    "Adam_Smith", "John_Maynard_Keynes", "Milton_Friedman",
    "Friedrich_Hayek", "Joseph_Schumpeter", "Amartya_Sen",
    "Galileo_Galilei", "Johannes_Kepler", "Nicolaus_Copernicus",
    "Tycho_Brahe", "Robert_Hooke", "Gottfried_Wilhelm_Leibniz",
    "Leonhard_Euler", "Carl_Friedrich_Gauss", "Bernhard_Riemann",
    "Henri_Poincare", "David_Hilbert", "Emmy_Noether",
    "Srinivasa_Ramanujan", "Kurt_Godel", "John_von_Neumann",
    "Claude_Shannon", "Norbert_Wiener", "Richard_Feynman",
    "Niels_Bohr", "Werner_Heisenberg", "Erwin_Schrodinger",
    "Paul_Dirac", "Max_Planck", "James_Clerk_Maxwell",
    "Michael_Faraday", "Andre-Marie_Ampere", "Georg_Ohm",
    "Heinrich_Hertz", "Guglielmo_Marconi", "Alexander_Graham_Bell",
    "Thomas_Edison", "Nikolai_Lobachevsky", "George_Boole",
    "Charles_Babbage", "Ada_Lovelace", "Grace_Hopper",
    "Tim_Berners-Lee", "Vint_Cerf", "Dennis_Ritchie",
    "Linus_Torvalds", "Steve_Jobs", "Bill_Gates",
    "Louis_Pasteur", "Robert_Koch", "Alexander_Fleming",
    "Jonas_Salk", "Francis_Crick", "James_Watson",
    "Rosalind_Franklin", "Barbara_McClintock", "Rachel_Carson",
    "Jane_Goodall", "David_Attenborough",
]

for article in WIKI_ARTICLES:
    urls.add(f"https://en.wikipedia.org/wiki/{article}")
    urls.add(f"https://en.wikipedia.org/wiki/Talk:{article}")

# Years
for year in range(1, 2027):
    urls.add(f"https://en.wikipedia.org/wiki/{year}")
for year in range(1800, 2027):
    for suffix in ["_in_science", "_in_literature", "_in_music", "_in_art",
                    "_in_film", "_in_television", "_in_sports", "_in_aviation",
                    "_in_spaceflight", "_in_archaeology"]:
        urls.add(f"https://en.wikipedia.org/wiki/{year}{suffix}")

# curid-based random articles — 500K of them
for _ in range(500000):
    curid = random.randint(1, 75000000)
    urls.add(f"https://en.wikipedia.org/w/index.php?curid={curid}")

# Special:Random with cache busters
for i in range(100000):
    urls.add(f"https://en.wikipedia.org/wiki/Special:Random?x={i}")

# Other language wikis
for lang in ["de", "fr", "es", "it", "pt", "ja", "zh", "ru", "ko", "ar",
             "nl", "sv", "pl", "vi", "id", "uk", "cs", "fi", "hu", "ro",
             "da", "no", "he", "th", "el", "bg", "hr", "sr", "sk", "sl",
             "et", "lt", "lv", "ca", "eu", "gl", "ms", "tl", "hi", "bn",
             "ta", "te", "mr", "ur", "fa", "tr"]:
    for _ in range(5000):
        curid = random.randint(1, 5000000)
        urls.add(f"https://{lang}.wikipedia.org/w/index.php?curid={curid}")

log(f"  Wikipedia: {len(urls):,} URLs")

# ========== 3. StackOverflow deep links ==========
log("Phase 3: StackOverflow...")
SO_TAGS = [
    "python", "javascript", "java", "c%23", "php", "android", "html", "css",
    "node.js", "sql", "mysql", "r", "reactjs", "c%2b%2b", "angular", "typescript",
    "linux", "git", "docker", "kubernetes", "rust", "go", "swift", "kotlin",
    "machine-learning", "deep-learning", "tensorflow", "pytorch", "numpy", "pandas",
    "django", "flask", "spring", "express", "vue.js", "svelte", "next.js",
    "aws", "azure", "google-cloud", "terraform", "ansible",
    "postgresql", "mongodb", "redis", "elasticsearch", "apache-kafka",
    "algorithms", "data-structures", "regex", "bash", "powershell",
    "networking", "security", "cryptography", "api", "rest", "graphql",
    "testing", "unit-testing", "selenium", "ci-cd", "jenkins",
]
for tag in SO_TAGS:
    for page in range(1, 201):
        urls.add(f"https://stackoverflow.com/questions/tagged/{tag}?tab=votes&page={page}")
        urls.add(f"https://stackoverflow.com/questions/tagged/{tag}?tab=newest&page={page}")

# Random SO question IDs (questions go up to ~80M)
for _ in range(200000):
    qid = random.randint(1, 80000000)
    urls.add(f"https://stackoverflow.com/questions/{qid}")

log(f"  StackOverflow: {len(urls):,} URLs")

# ========== 4. GitHub trending & topics ==========
log("Phase 4: GitHub...")
GH_TOPICS = [
    "python", "javascript", "typescript", "rust", "go", "java", "cpp",
    "machine-learning", "deep-learning", "artificial-intelligence",
    "web", "api", "cli", "database", "security", "devops", "linux",
    "react", "vue", "angular", "svelte", "nextjs", "django", "flask",
    "docker", "kubernetes", "terraform", "ansible",
    "algorithms", "data-structures", "compilers", "operating-systems",
]
for topic in GH_TOPICS:
    urls.add(f"https://github.com/topics/{topic}")
    for page in range(1, 21):
        urls.add(f"https://github.com/topics/{topic}?page={page}")

# Popular GitHub repos README pages
GH_REPOS = [
    "torvalds/linux", "python/cpython", "rust-lang/rust", "golang/go",
    "tensorflow/tensorflow", "pytorch/pytorch", "microsoft/vscode",
    "facebook/react", "vuejs/vue", "angular/angular",
    "nodejs/node", "django/django", "pallets/flask",
    "kubernetes/kubernetes", "docker/compose",
    "apache/spark", "elastic/elasticsearch",
    "redis/redis", "postgres/postgres",
    "openai/gpt-2", "huggingface/transformers",
    "scikit-learn/scikit-learn", "pandas-dev/pandas", "numpy/numpy",
]
for repo in GH_REPOS:
    urls.add(f"https://github.com/{repo}")
    urls.add(f"https://github.com/{repo}/wiki")
    urls.add(f"https://github.com/{repo}/issues")
    urls.add(f"https://github.com/{repo}/pulls")

log(f"  GitHub: {len(urls):,} URLs")

# ========== 5. Reddit deep links ==========
log("Phase 5: Reddit...")
SUBREDDITS = [
    "science", "technology", "worldnews", "askscience", "explainlikeimfive",
    "todayilearned", "history", "space", "physics", "chemistry", "biology",
    "math", "programming", "machinelearning", "datascience", "philosophy",
    "books", "movies", "music", "food", "travel", "fitness", "medicine",
    "engineering", "economics", "psychology", "linguistics", "anthropology",
    "archaeology", "geology", "astronomy", "environment", "energy",
    "climate", "oceanography", "neuroscience", "genetics", "ecology",
    "evolution", "botanicalporn", "mycology", "entomology",
    "askhistorians", "askphilosophy", "askengineers", "askeconomics",
    "learnprogramming", "compsci", "netsec", "reverseengineering",
    "artificial", "deeplearning", "statistics", "dataisbeautiful",
    "mapporn", "historyporn", "earthporn", "spaceporn", "cityporn",
    "architectureporn", "educationalgifs", "interestingasfuck",
    "damnthatsinteresting", "coolguides", "futurology",
    "collapse", "geopolitics", "neutralpolitics", "truereddit",
    "depthub", "bestof", "changemyview", "unpopularopinion",
    "documentaries", "lectures", "mealtimevideos",
    "python", "javascript", "rust", "golang", "java", "cpp",
    "linux", "sysadmin", "devops", "homelab", "selfhosted",
    "mechanicalkeyboards", "buildapc", "hardware", "overclocking",
    "cooking", "recipes", "seriouseats", "breadit", "fermentation",
    "gardening", "permaculture", "composting",
    "photography", "videography", "filmmakers",
    "writing", "screenwriting", "worldbuilding",
    "chess", "boardgames", "rpg", "dnd",
    "running", "cycling", "swimming", "climbing", "hiking",
    "camping", "backpacking", "ultralight",
]
for sub in SUBREDDITS:
    urls.add(f"https://www.reddit.com/r/{sub}/")
    urls.add(f"https://old.reddit.com/r/{sub}/")
    for sort in ["top", "hot", "new", "controversial"]:
        urls.add(f"https://www.reddit.com/r/{sub}/{sort}/")
        urls.add(f"https://old.reddit.com/r/{sub}/{sort}/")
    for time in ["all", "year", "month", "week"]:
        urls.add(f"https://www.reddit.com/r/{sub}/top/?t={time}")

log(f"  Reddit: {len(urls):,} URLs")

# ========== 6. News archive deep date links ==========
log("Phase 6: News archives...")
NEWS_DOMAINS = [
    "bbc.com/news", "reuters.com", "apnews.com", "theguardian.com",
    "nytimes.com", "washingtonpost.com", "cnn.com", "aljazeera.com",
    "npr.org", "theatlantic.com", "wired.com", "arstechnica.com",
    "latimes.com", "chicagotribune.com", "usatoday.com",
]
NEWS_SECTIONS = [
    "world", "us", "uk", "business", "technology", "science",
    "health", "entertainment", "sports", "politics", "environment",
    "education", "opinion", "analysis", "features", "investigations",
    "asia", "europe", "africa", "americas", "middle-east",
]
for site in NEWS_DOMAINS:
    for section in NEWS_SECTIONS:
        for year in range(2015, 2027):
            for month in range(1, 13):
                urls.add(f"https://www.{site}/{section}/{year}/{month:02d}")
                for day in [1, 5, 10, 15, 20, 25]:
                    urls.add(f"https://www.{site}/{section}/{year}/{month:02d}/{day:02d}")

log(f"  News: {len(urls):,} URLs")

# ========== 7. Academic & research sites ==========
log("Phase 7: Academic URLs...")
UNIVERSITIES = [
    "mit.edu", "stanford.edu", "harvard.edu", "berkeley.edu", "caltech.edu",
    "princeton.edu", "yale.edu", "columbia.edu", "cornell.edu", "upenn.edu",
    "uchicago.edu", "duke.edu", "northwestern.edu", "jhu.edu", "cmu.edu",
    "gatech.edu", "umich.edu", "uw.edu", "ucla.edu", "ucsd.edu",
    "uiuc.edu", "purdue.edu", "wisc.edu", "utexas.edu", "psu.edu",
    "cam.ac.uk", "ox.ac.uk", "imperial.ac.uk", "ucl.ac.uk", "ed.ac.uk",
    "ethz.ch", "epfl.ch", "tum.de", "lmu.de",
    "anu.edu.au", "unimelb.edu.au", "utoronto.ca", "ubc.ca", "mcgill.ca",
]
UNI_PATHS = [
    "research", "news", "about", "academics", "departments", "faculty",
    "library", "publications", "programs", "courses", "events",
    "computer-science", "physics", "mathematics", "chemistry", "biology",
    "engineering", "medicine", "law", "business", "economics",
    "history", "philosophy", "psychology", "linguistics", "sociology",
    "electrical-engineering", "mechanical-engineering", "civil-engineering",
    "environmental-science", "materials-science", "biomedical-engineering",
    "neuroscience", "cognitive-science", "political-science",
    "public-health", "epidemiology", "statistics",
]
for uni in UNIVERSITIES:
    for path in UNI_PATHS:
        urls.add(f"https://www.{uni}/{path}/")
        urls.add(f"https://{uni}/{path}/")

# arXiv papers (IDs go up to ~2503.xxxxx)
for year in range(14, 26):
    for month in range(1, 13):
        for paper in range(1, 501):
            urls.add(f"https://arxiv.org/abs/{year:02d}{month:02d}.{paper:05d}")

# PubMed articles
for _ in range(200000):
    pmid = random.randint(1, 40000000)
    urls.add(f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/")

log(f"  Academic: {len(urls):,} URLs")

# ========== 8. Misc high-quality sites ==========
log("Phase 8: Misc sites...")

# Medium articles by topic
MEDIUM_TAGS = [
    "artificial-intelligence", "machine-learning", "data-science", "programming",
    "python", "javascript", "technology", "science", "startup", "design",
    "productivity", "self-improvement", "writing", "marketing", "business",
    "blockchain", "cryptocurrency", "cybersecurity", "ux-design", "web-development",
    "deep-learning", "nlp", "computer-vision", "robotics", "iot",
    "cloud-computing", "devops", "microservices", "api", "databases",
    "algorithms", "software-engineering", "system-design", "architecture",
    "leadership", "management", "culture", "education", "health",
    "mental-health", "neuroscience", "psychology", "philosophy", "history",
    "economics", "politics", "climate-change", "sustainability", "energy",
]
for tag in MEDIUM_TAGS:
    urls.add(f"https://medium.com/tag/{tag}")
    urls.add(f"https://medium.com/tag/{tag}/latest")
    urls.add(f"https://medium.com/tag/{tag}/top/all-time")
    for year in range(2020, 2027):
        for month in range(1, 13):
            urls.add(f"https://medium.com/tag/{tag}/archive/{year}/{month:02d}")

# HackerNews items (IDs go up to ~40M+)
for _ in range(200000):
    item_id = random.randint(1, 42000000)
    urls.add(f"https://news.ycombinator.com/item?id={item_id}")

# IMDb titles
for _ in range(100000):
    tid = random.randint(1, 30000000)
    urls.add(f"https://www.imdb.com/title/tt{tid:07d}/")

# Goodreads books
for _ in range(50000):
    bid = random.randint(1, 70000000)
    urls.add(f"https://www.goodreads.com/book/show/{bid}")

log(f"  Misc: {len(urls):,} URLs")

# ========== 9. Documentation sites ==========
log("Phase 9: Documentation sites...")
PYTHON_MODULES = [
    "os", "sys", "json", "re", "math", "datetime", "collections", "itertools",
    "functools", "pathlib", "asyncio", "typing", "logging", "unittest",
    "argparse", "subprocess", "threading", "multiprocessing", "socket",
    "http", "urllib", "xml", "csv", "sqlite3", "hashlib", "random",
    "statistics", "decimal", "array", "queue", "heapq", "bisect",
    "struct", "io", "time", "string", "textwrap", "difflib",
    "enum", "dataclasses", "abc", "contextlib", "warnings",
    "copy", "pprint", "reprlib", "operator", "inspect",
    "importlib", "pkgutil", "zipimport", "compileall",
    "dis", "ast", "symtable", "token", "tokenize",
    "pickle", "shelve", "marshal", "dbm",
    "gzip", "bz2", "lzma", "zipfile", "tarfile",
    "tempfile", "glob", "fnmatch", "shutil",
    "filecmp", "stat", "fileinput",
    "signal", "mmap", "ctypes", "select",
    "ssl", "ftplib", "smtplib", "imaplib",
    "cgi", "html", "webbrowser",
]
for mod in PYTHON_MODULES:
    urls.add(f"https://docs.python.org/3/library/{mod}.html")

MDN_PATHS = [
    "HTML", "CSS", "JavaScript", "API", "HTTP", "SVG", "MathML",
    "Web/HTML/Element", "Web/CSS/Reference", "Web/JavaScript/Reference",
    "Web/API/Document", "Web/API/Window", "Web/API/Element",
    "Web/API/Fetch_API", "Web/API/Canvas_API", "Web/API/WebSocket",
    "Web/API/Web_Workers_API", "Web/API/Service_Worker_API",
    "Web/API/IndexedDB_API", "Web/API/Geolocation_API",
    "Web/API/Web_Audio_API", "Web/API/WebGL_API", "Web/API/WebRTC_API",
    "Web/HTTP/Headers", "Web/HTTP/Methods", "Web/HTTP/Status",
]
for path in MDN_PATHS:
    urls.add(f"https://developer.mozilla.org/en-US/docs/{path}")

log(f"  Docs: {len(urls):,} URLs")

# ========== 10. Generate numeric URL patterns for high-throughput sites ==========
log("Phase 10: Numeric pattern generation for remaining URLs...")

# Britannica articles
for _ in range(100000):
    word = random.choice(WORDS + [a.replace("_", "-").lower() for a in WIKI_ARTICLES[:200]])
    urls.add(f"https://www.britannica.com/topic/{word}")
    urls.add(f"https://www.britannica.com/science/{word}")
    urls.add(f"https://www.britannica.com/technology/{word}")
    urls.add(f"https://www.britannica.com/place/{word}")
    urls.add(f"https://www.britannica.com/biography/{word}")
    urls.add(f"https://www.britannica.com/event/{word}")

# Additional random Wikipedia curids to hit 10M
remaining = 10000000 - len(urls)
if remaining > 0:
    log(f"  Generating {remaining:,} more Wikipedia curid URLs to reach 10M...")
    batch = set()
    while len(batch) < remaining:
        curid = random.randint(1, 75000000)
        batch.add(f"https://en.wikipedia.org/w/index.php?curid={curid}")
        if len(batch) % 500000 == 0:
            log(f"    {len(batch):,} / {remaining:,}")
    urls.update(batch)

log(f"  Final total: {len(urls):,} URLs")

# ========== WRITE OUTPUT ==========
url_list = list(urls)
random.shuffle(url_list)
log(f"Writing {len(url_list):,} URLs to {OUTPUT}...")
with open(OUTPUT, "w") as f:
    for url in url_list:
        f.write(url + "\n")

sz = os.path.getsize(OUTPUT)
log(f"Done! {len(url_list):,} unique URLs written ({sz/(1024*1024):.1f}MB)")
