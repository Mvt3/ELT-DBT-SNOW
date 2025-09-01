#Script de actualización de CSVs en S3, analizara a su vez si hay cambios tambien en la estructura de terraform.
#Tambien ejecutara los test y run de dbt (este proyecto se ejecuta todo de manera local)

#Liberias
import subprocess
import boto3
import pandas as pd
import os
import tempfile



# Configuraciones
S3_BUCKET = "netflix-datasets-bs3"
LOCAL_CSVS = ["./Datasets/ml-20m/genome-scores.csv", "./Datasets/ml-20m/genome-tags.csv", "./Datasets/ml-20m/links.csv", "./Datasets/ml-20m/movies.csv", "./Datasets/ml-20m/ratings.csv", "./Datasets/ml-20m/tags.csv"]  
AWS_REGION = "us-east-1"
TERRAFORM_DIR = "./terraform"   
DBT_PROJECT_DIR = "./dbt/netflix"


# Cliente S3
s3 = boto3.client("s3", region_name=AWS_REGION)

def run_terraform():
    # Ejecutar terraform init
    subprocess.run(["terraform", "init"], cwd=TERRAFORM_DIR,check=True)

    # Ver si hay cambios
    result = subprocess.run(
        ["terraform", "plan", "-detailed-exitcode"], 
        cwd=TERRAFORM_DIR, 
        capture_output=True
    )
    if result.returncode == 2:
        print("Hay cambios en Terraform, aplicando...")
        subprocess.run(
            ["terraform", "apply", "-auto-approve"], 
            cwd=TERRAFORM_DIR, 
            check=True
        )
    elif result.returncode == 0:
        print("No hay cambios en Terraform.")
    else:
        print("Error en terraform plan")
        print(result.stderr.decode())
        raise SystemExit(1)


# si hay informacion nueva en los csv se actualizaran con append los que ya existen en S3
def update_csv_in_s3(local_csv, bucket):
    
    s3_key = os.path.basename(local_csv)

    # Cargar CSV local
    df_local = pd.read_csv(local_csv)

    try:
        # Descargar CSV actual de S3
        tmp_s3 = os.path.join(tempfile.gettempdir(), "tmp_s3.csv")
        s3.download_file(bucket, s3_key, tmp_s3)
        df_s3 = pd.read_csv(tmp_s3)

        # Concatenar y eliminar duplicados
        df_final = pd.concat([df_s3, df_local]).drop_duplicates()
        
    except s3.exceptions.ClientError:
        # Si no existe en S3, solo usar el local
        df_final = df_local

     # Guardar y subir a S3
     # Verificar si cambió algo antes de subir
    if 'df_s3' not in locals() or not df_final.equals(df_s3):
        tmp_final = os.path.join(tempfile.gettempdir(), "tmp_final.csv")
        df_final.to_csv(tmp_final, index=False)
        s3.upload_file(tmp_final, bucket, s3_key)
        print(f"CSV actualizado en s3://{bucket}/{s3_key}")
    else:
        print(f"No hay cambios en {s3_key}, no se sube.")

# Función para ejecutar dbt
def run_dbt(target: str):

    env = os.environ.copy()
    env['DBT_PROFILES_DIR'] = r'C:\Users\mauro\.dbt'

    try:
    
        if target == "prod":
            # Solo modelos dim_ y fct_ en prod
            run_cmd = ["dbt", "run", "--target", target, "--select", "dim_* fct_*"]
        else:
            # En dev ejecuta todo
            run_cmd = ["dbt", "run", "--target", target]
            test_cmd = ["dbt", "test", "--target", target]

        # Ejecuta los modelos
        subprocess.run(run_cmd, check=True, cwd=DBT_PROJECT_DIR, env=env)

        # Ejecuta los tests
        if test_cmd:
            subprocess.run(test_cmd, check=True, cwd=DBT_PROJECT_DIR, env=env)

    except subprocess.CalledProcessError as e:
        print("Error ejecutando dbt:", e)
        raise
   


if __name__ == "__main__":
    
   #Ejecucion de terraform
    run_terraform()
    for csv_file in LOCAL_CSVS:
        update_csv_in_s3(csv_file, S3_BUCKET)


   #Ejecucion de dbt)
    try:
        run_dbt("dev")
    except subprocess.CalledProcessError:
        print("Tests fallaron en DEV, no se promociona a Prod")
        raise SystemExit(1)

    # dbt en Prod (solo si DEV pasó)
    run_dbt("prod")
    print("Pipeline completado con éxito: datos en Prod listos para Looker")

