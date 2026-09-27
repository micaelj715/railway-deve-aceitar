# Bac Bo Bot — Telegram (pronto para Railway)

Bot que acompanha a mesa Bac Bo (TipMiner) e envia ENTRADA, GALE e GREEN/RED para o Telegram.
Sem painel web, sem porta: corre como *worker*. Não prevê resultados — cada rodada é independente.

## Deploy no Railway (passo a passo)
1. Suba esta pasta para um repositório no GitHub (o `config.json` fica fora, pelo `.gitignore`).
2. No Railway: **New Project → Deploy from GitHub repo** e escolha o repositório.
   O `railway.json` já define o arranque (`python app.py`), 1 réplica e reinício automático em caso de falha.
3. Em **Variables** adicione:
   | Variável | Exemplo | Obrigatória |
   |---|---|---|
   | `TELEGRAM_TOKEN` | `123456:ABC...` (do @BotFather) | sim |
   | `TELEGRAM_CHAT_ID` | `-1001234567890` | sim |
   | `RAILWAY_DEPLOYMENT_OVERLAP_SECONDS` | `0` | recomendado (evita 2 bots ao mesmo tempo no redeploy) |
   | `PADROES` | `PPB oposto 1 Quebra; BB mesmo 0 Seguir` | opcional |
   | `TELEGRAM_ADMINS` | `111111111,222222222` | opcional |
4. **Recomendado — Volume** (para os padrões e o placar não se perderem a cada deploy):
   no serviço, **Settings → Volumes → Add Volume**, caminho `/data`. O bot deteta o volume sozinho.
5. Não é preciso gerar domínio nem configurar porta.

Quando arranca, o bot envia no chat **"🤖 BOT INICIADO ✅ — Tudo a funcionar, sem erros."**
Se houver algum problema envia "⚠️ Arrancou, mas com avisos" com o motivo. Os detalhes ficam em **Deployments → Logs**.

### Sem volume?
Funciona na mesma, mas o que foi criado com `/novo` apaga-se no próximo deploy.
Nesse caso use a variável `PADROES` — os padrões são recriados a cada arranque.

## Todas as opções por variável
Qualquer opção do `config.example.json` pode ser uma variável com o nome em MAIÚSCULAS:
`MAX_GALE=1`, `TELEGRAM_PRE_ALERT=false`, `TIE_AS_GREEN=true`, `ENGINE_SIGNALS=false`, etc.

## Comandos no Telegram
| Comando | O que faz |
|---|---|
| `/status` | estado da coleta, rodadas, padrões, onde ficam os dados |
| `/placar` | estatísticas de green/red |
| `/padroes` | lista numerada dos seus padrões com acerto no histórico |
| `/novo PPB oposto 1 Nome` | cria padrão (P, B, T, * · entrada P/B/T/mesmo/oposto · gale 0-2) |
| `/ligar n` · `/desligar n` · `/apagar n` | gere o padrão número n |
| `/pausar` · `/retomar` | desliga/liga o envio de sinais |
| `/zerar` | zera o placar |

Num grupo ou canal, o bot tem de ser **administrador**.

## Correr no PC
```
pip install -r requirements.txt
copy config.example.json config.json   (preencha token e chat id)
python app.py
```
