"""Страж целиком: хранилище + случайный ключ + реальный Lighter RH → должен понятно отказать на проверке ключа."""
import json, os, subprocess, sys, time
from pathlib import Path
G = Path(sys.argv[1]); acc = sys.argv[2]
sys.path.insert(0, str(G))
os.chdir(G)
import vault, lighter_signer
priv, pub = lighter_signer.generate_api_key()
v = vault.Vault.create("probe password 123", G / "guardian.vault")
v.data.update({"W1_RH_API_PRIVATE_KEY": priv, "W1_RH_API_KEY_INDEX": "4", "W1_RH_ACCOUNT_INDEX": acc, "W1_NAME": "Проба"})
v.save()
pw = G / "pw.txt"; pw.write_text("probe password 123\n"); os.chmod(pw, 0o600)
(G / ".env").write_text("DRY_RUN=true\n")
env = dict(os.environ, GUARDIAN_PASSWORD_FILE=str(pw))
t = time.time()
r = subprocess.run([sys.executable, "guardian.py"], env=env, capture_output=True, text=True, timeout=90)
res = {"rc": r.returncode, "secs": round(time.time() - t, 1), "tail": (r.stdout + r.stderr)[-2500:],
       "leaked_key": priv[2:20] in (r.stdout + r.stderr)}
print(json.dumps(res, ensure_ascii=False))
print("::notice::E2E " + json.dumps(res, ensure_ascii=False)[:3800])
