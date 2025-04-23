# teste_url.py

print("Iniciando teste de URL...\n")

dominio = "https://mi.tv"
pais = "br"
rota = "canais"
canal = "canal-brasil"

full_url = f"{dominio}/{pais}/{rota}/{canal}"
print(f"URL montada: {full_url!r}")
