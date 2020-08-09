# StanislawAniolaProfilRecruitmentExercisePython
Repository contains python script needed in Profil recruitment process.

1. Download Project:
git clone https://github.com/StanislawAniola/StanislawAniolaProfilRecruitmentExercisePython.git

2. Install external packages:
- navigate to where project has been downloaded
- pip install -r requirements.txt

3. inf
Project use:
- requests,
- json,
- datatime,
- dateutil.parser,
- peewee,
- click

4. Choose data from file or api:
- line: 63
- api_file = choose_api_file()

- enter 'api' into parenthesis. By default it's 'file'

5. Command Line user options:

INITIALIZING DATABASE:
1. create database
- format: python sa_py.py init

2. inserting data to database
- format: python sa_py.py insert
__________________________________

OPERATIONS ON DATABASE:
1. procent kobiet i mężczyzn
- format: python sa_py.py gen_perc

2. średnia wieku
- format1: python sa_py.py gen_call | total average age
- format2: python sa_py.py gen_call --gen_call male | male average age
- format3: python sa_py.py gen_call --gen_call female | female average age

3. N najbardziej popularnych miast
- format1: python sa_py.py city | most common city
- format2-n: python sa_py.py city --count n | (n = number) n most common cities

4. N najpopularniejszych haseł
- format1: python sa_py.py pass_com | most common password
- format2-n: python sa_py.py pass_com --count n | (n = number) n most common passwords

5. wszystkich użytkowników którzy urodzili się w zakresie dat podanym jako parametr
- format: python sa_py.py birth --start YYYY-MM-DD --end YYYY-MM-DD

6. najbezpieczniejsze hasło
- format: pass_strong
