# ETL Pipeline med AES Kryptografi

## Oversigt

Dette projekt er en udvidelse af den originale ETL pipeline, nu med **AES kryptering** af transformeret data.

## Nye Filer

### Krypterings Moduler:
- **`security_module.py`** - Indeholder 3 krypteringsmetoder:
  - AES-GCM (valgt metode)
  - AES-CBC
  - Fernet (AES-CBC wrapper)

- **`load_module_encrypted.py`** - Load modul med kryptering/dekryptering
- **`main_encrypted.py`** - Hoved script med kryptering

### Nøgle Fil:
- **`encryption.key`** - 32-byte AES krypteringsnøgle (autogenereret)

## Kør Den Krypterede Pipeline

```bash
python main_encrypted.py
```

## Hvad Sker Der?

1. **Extract**: Henter iris.csv data
2. **Transform**: Filtrer til Iris-setosa (50 rækker)
3. **Load (KRYPTERET)**:
   - Krypterer alle numeriske kolonner med AES-GCM
   - Gemmer til CSV: `transformed_iris_encrypted.csv`
   - Gemmer til MySQL: `iris_setosa_encrypted` tabel
4. **Visualize (DEKRYPTERET)**:
   - Læser krypteret data fra MySQL
   - Dekrypterer data
   - Viser 3 grafer

## Valg af Krypteringsmetode

**Valgt: AES-GCM**

### Hvorfor AES-GCM?

✅ **AEAD** - Authenticated Encryption (indbygget integritet)
✅ **Hurtigst** - Hardware acceleration
✅ **Nemt** - Ingen padding nødvendig
✅ **Moderne** - Anbefalet til nye systemer
✅ **Sikkert** - Beskytter mod manipulation

### Hvorfor IKKE de andre?

**AES-CBC:**
- ❌ Kræver separat HMAC
- ❌ Padding nødvendig (PKCS7)
- ❌ Langsommere

**Fernet:**
- ❌ Større overhead
- ❌ Langsommere
- ❌ Mindre fleksibel

## Kryptering i Praksis

### CSV Fil (før kryptering):
```
sepal_length,sepal_width,petal_length,petal_width,species
5.1,3.5,1.4,0.2,Iris-setosa
```

### CSV Fil (efter kryptering):
```
sepal_length,sepal_width,petal_length,petal_width,species
qtpPAOHPCOVBc83k...==,hzVm0/S4+5dm...==,cMNos6+19Gp...==,aKB8F9Il...==,Iris-setosa
```

## MySQL Database

**Tabel**: `iris_setosa_encrypted`

**Struktur**:
- `sepal_length` TEXT (krypteret)
- `sepal_width` TEXT (krypteret)
- `petal_length` TEXT (krypteret)
- `petal_width` TEXT (krypteret)
- `species` VARCHAR(50) (ikke krypteret)

## Sikkerhed

⚠️ **VIGTIGT**: Gem `encryption.key` sikkert!
- Uden nøglen kan data ikke dekrypteres
- Del ALDRIG nøglen i version control
- I produktion: brug key management system

## Dependencies

```bash
pip install cryptography>=41.0.0
```

## Sammenligning: Original vs Krypteret

| Feature | Original | Krypteret |
|---------|----------|-----------|
| Script | `main_pandas.py` | `main_encrypted.py` |
| CSV Output | `transformed_iris.csv` | `transformed_iris_encrypted.csv` |
| MySQL Tabel | `iris_setosa` | `iris_setosa_encrypted` |
| Data Format | Plain text | AES-GCM encrypted |
| Sikkerhed | Ingen | AES-256 encryption |

## Opgave Krav Opfyldt

✅ Kopi af original kode
✅ Security modul med 3 metoder
✅ AES-GCM implementation
✅ AES-CBC implementation
✅ Fernet implementation
✅ Kryptering af CSV data
✅ Kryptering af MySQL data
✅ Dekryptering til visualisering
✅ Forklaring af metodeval
