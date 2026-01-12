import os
import pandas as pd

pastas = ["csvs", "csvspos"]

arquivos_por_pasta = {
    pasta: {arq for arq in os.listdir(pasta) if arq.endswith(".csv")}
    for pasta in pastas
}

arquivos_duplicados = arquivos_por_pasta["csvs"].intersection(
    arquivos_por_pasta["csvspos"]
)

print("Arquivos ignorados por duplicidade:")
print(len(arquivos_duplicados))

lista_dfs = []
i = 0

for pasta in pastas:
    for arquivo in arquivos_por_pasta[pasta]:
        if arquivo not in arquivos_duplicados:
            i += 1
            print(i)
            caminho_arquivo = os.path.join(pasta, arquivo)
            df = pd.read_csv(caminho_arquivo)
            lista_dfs.append(df)

df_final = pd.concat(lista_dfs, ignore_index=True)

print(df_final.head())

df_final.to_csv("csv_concatenado.csv", index=False)
