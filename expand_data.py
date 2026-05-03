import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Lire le fichier existant
df_existing = pd.read_csv('pharmacie_raw.csv')

print(f"Lignes existantes: {len(df_existing)}")

# Données réalistes tunisiennes
villes = ['Tunis', 'Ariana', 'Ben Arous', 'Manouba', 'Nabeul', 'Hammamet', 'Sousse', 'Monastir', 'Sfax', 'Gafsa', 'Tozeur', 'Kebili', 'Medenine', 'Tataouine', 'Djerba', 'Bizerte', 'Beja', 'Jendouba', 'Kairouan', 'Kasserine', 'Sidi Bouzid', 'Mahdia', 'Gabes']
prenoms = ['Amine', 'Mariem', 'Mohammed', 'Fatima', 'Ali', 'Nadia', 'Salma', 'Khaled', 'Hana', 'Omar', 'Youssef', 'Leila', 'Samir', 'Zainab', 'Karim', 'Aïda', 'Riadh', 'Wicem', 'Sabrine', 'Tarek']
noms = ['Bouzid', 'Jebali', 'Ben Ali', 'Souissi', 'Gharbi', 'Ayachi', 'Triki', 'Khouja', 'Baccar', 'Hamza', 'Amri', 'Ghanem', 'Oueslati', 'Sfaxi', 'Mahjoubi', 'Kacem', 'Elloumi', 'Mabrouk', 'Baccouche', 'Hachicha']
mutuelles = ['CNAM', 'STAR', 'GAT', 'SNCFT', 'CRDA', 'CNSS', 'CNRPS', 'sans']
medicaments = ['Ventoline 100mcg', 'Aspirin 100mg', 'Augmentin 1g', 'Levothyrox 50mcg', 'Coversyl 5mg', 'Metformine 1000mg', 'Lisinopril 10mg', 'Oméprazole 20mg', 'Amoxicilline 500mg', 'Ibuprofen 200mg', 'Atorvastatine 20mg', 'Ramipril 5mg', 'Sertraline 50mg', 'Fluoxetine 20mg', 'Clopidogrel 75mg']
medecins = ['Dr. Hamrouni', 'Dr. Elloumi', 'Dr. Mabrouk', 'Dr. Baccouche', 'Dr. Sfaxi', 'Dr. Oueslati', 'Dr. Khouja', 'Dr. Ghanem', 'Dr. Ayachi', 'Dr. Triki']
specialites = ['Pneumologue', 'Endocrinologue', 'Diabétologue', 'Cardiologue', 'Gastrologue', 'Néphrologue', 'Rhumatologue', 'Neurologue', 'Pédiatre', 'Généraliste']
renouvellement = ['oui', 'non', 'Non', 'NON', 'OUI']

# Générer 400 lignes
np.random.seed(42)
new_data = []

for i in range(400):
    patient_id = f"PAT{5000 + i}"
    prenom = random.choice(prenoms)
    nom = random.choice(noms)
    age = np.random.randint(0, 100)
    poids_kg = round(np.random.uniform(50, 120), 1)
    ville = random.choice(villes)
    telephone = f"+216 {np.random.randint(20, 99)} {np.random.randint(100000, 999999)}"
    email = f"{prenom.lower()}.{nom.lower()}@email.tn"
    mutuelle = random.choice(mutuelles)
    medicament = random.choice(medicaments)
    dose_par_jour = round(np.random.uniform(0.5, 3), 1)
    duree_traitement = np.random.randint(5, 30)
    prix_DT = round(np.random.uniform(10, 200), 2)
    medecin = random.choice(medecins)
    specialite = random.choice(specialites)
    
    # Dates réalistes
    date_ordonnance = (datetime(2023, 1, 1) + timedelta(days=np.random.randint(0, 730))).strftime('%Y-%m-%d')
    date_dispensation = (datetime.strptime(date_ordonnance, '%Y-%m-%d') + timedelta(days=np.random.randint(0, 5))).strftime('%Y-%m-%d')
    
    renouvellement_val = random.choice(renouvellement)
    
    new_data.append({
        'patient_id': patient_id,
        'prenom': prenom,
        'nom': nom,
        'age': age,
        'poids_kg': poids_kg,
        'ville': ville,
        'telephone': telephone,
        'email': email,
        'mutuelle': mutuelle,
        'medicament': medicament,
        'dose_par_jour': dose_par_jour,
        'duree_traitement': duree_traitement,
        'prix_DT': prix_DT,
        'medecin': medecin,
        'specialite': specialite,
        'date_ordonnance': date_ordonnance,
        'date_dispensation': date_dispensation,
        'renouvellement': renouvellement_val
    })

df_new = pd.DataFrame(new_data)

# Combiner et sauvegarder
df_combined = pd.concat([df_existing, df_new], ignore_index=True)
df_combined.to_csv('pharmacie_raw.csv', index=False)

print(f"✅ Lignes ajoutées: {len(df_new)}")
print(f"✅ Total de lignes: {len(df_combined)}")
print("✅ Fichier sauvegardé!")
