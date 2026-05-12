/**
 * Синхронизация чата + авторитетное состояние (боты NPC, модерация, счётчик для роли).
 * Порт CHAT_PORT или 8787. Состояние админских патчей + NPC сохраняется в CHAT_STATE_PATH (JSON).
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { randomUUID } from 'crypto';
import { WebSocketServer } from 'ws';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.CHAT_PORT || 8787);
const STATE_PATH =
  process.env.CHAT_STATE_PATH || path.join(__dirname, 'chat-state.json');
const MAX_MESSAGES = 4000;
const MAX_FRIEND_REQUESTS = 2000;

/** @type {unknown[]} */
const messages = [];
/** @type {unknown[]} */
const friendRequests = [];
/** @type {unknown[]} */
const profileReactions = [];
/** @type {unknown[]} */
const orders = [];

const NPC_MESSAGES = [
  ['Странник', 'Эй, кто-нибудь слышал о пещере за северными горами? Говорят, там живёт дракон!'],
  ['Кузнец Борин', 'Мой молот снова сломался... проклятые гоблины напали на склад!'],
  ['Торговец Дэйв', 'Цены на мифрил снова ползут вверх. Закупайтесь пока есть возможность!'],
  ['Охотник Рей', 'Вчера видел следы тролля возле реки. Будьте осторожны там...'],
  ['Эльфийка Лэйя', 'Лунный цветок расцветает только раз в столетие. Это знак перемен...'],
  ['Маг Зэйрос', 'Мои изыскания продвигаются! Скоро раскрою тайну исчезнувшего города.'],
  ['Барменша Грейс', 'Ещё одна кружка эля? Сегодня скидки для искателей приключений!'],
  ['Рыцарь Торвальд', 'Орден Стальной Руки объявил поход на восток. Ищем добровольцев!'],
  ['Ведьма Милла', 'Звёзды говорят... грядут тёмные времена. Запасайтесь амулетами.'],
  ['Гном Кракт', 'Нашёл жилу руды глубоко под горой! Но туда не добраться без верёвки и смелости.'],
  ['Бард Силас', 'Слышали мою новую балладу? Она о герое, победившем Короля-лича!'],
  ['Торговец Дэйв', 'Кто купит зачарованные сапоги? Всего за 300 золотых!'],
  ['Странник', 'Не знаете хорошего мага-целителя? Меня укусил мертвяк...'],
  ['Охотник Рей', 'Сегодня удачная охота! Продаю шкуры виверны по честной цене.'],
  ['Кузнец Борин', 'Приносите сюда своё оружие! Заточу лучше, чем эльфийские мастера!'],
  ['Маг Зэйрос', 'Не трогайте старые руины на холме. Там просыпается нечто древнее...'],
  ['Бард Силас', 'Ха! Слышали анекдот про дракона и принцессу? Она сама его съела!'],
  ['Ведьма Милла', 'Продаю обереги от нежити. Цена — одна услуга или пять золотых.'],
  ['Рыцарь Торвальд', 'Честь и доблесть — вот оружие настоящего рыцаря. И хороший меч.'],
  ['Эльфийка Лэйя', 'Люди такие торопливые... Мне уже триста лет, а вы всё куда-то спешите.'],
  ['Гном Кракт', 'Борода — гордость гнома! Кто тронет мою бороду — пожалеет!'],
  ['Барменша Грейс', 'Тише там! Это таверна, а не поле битвы. Ещё раз подерётесь — выгоню всех!'],
  ['Странник', 'Ищу попутчиков до Серебряного Предела. Дорога опасная, одному не пройти.'],
  ['Охотник Рей', 'Говорят, в лесу Теней завелись оборотни. Я видел следы... огромные.'],
  ['Торговец Дэйв', 'Налетай! Редкие артефакты из дальних земель! Только сегодня!'],
];

const defaultPersist = {
  npcBotEnabled: true,
  npcIntervalMinMs: 15000,
  npcIntervalMaxMs: 45000,
  /** @type {Record<string, Record<string, unknown>>} */
  accountPatches: {},
};

function loadPersist() {
  try {
    const raw = fs.readFileSync(STATE_PATH, 'utf8');
    const j = JSON.parse(raw);
    return {
      ...defaultPersist,
      ...j,
      accountPatches:
        j && typeof j.accountPatches === 'object' && j.accountPatches
          ? j.accountPatches
          : {},
    };
  } catch {
    return { ...defaultPersist, accountPatches: {} };
  }
}

let persist = loadPersist();

function savePersist() {
  try {
    fs.writeFileSync(
      STATE_PATH,
      JSON.stringify(persist, null, 0),
      'utf8'
    );
  } catch (e) {
    console.warn('[chat-server] savePersist failed:', e?.message || e);
  }
}

function clampNpcIntervals(minMs, maxMs) {
  let min = Math.max(3000, Math.min(Number(minMs) || 15000, 600_000));
  let max = Math.max(
    min + 1000,
    Math.min(Number(maxMs) || 45000, 600_000)
  );
  return { min, max };
}

/** @type {ReturnType<typeof setTimeout> | null} */
let npcServerTimer = null;

function clearNpcServer() {
  if (npcServerTimer) {
    clearTimeout(npcServerTimer);
    npcServerTimer = null;
  }
}

function scheduleNpcServer() {
  clearNpcServer();
  if (!persist.npcBotEnabled) return;
  const { min, max } = clampNpcIntervals(
    persist.npcIntervalMinMs,
    persist.npcIntervalMaxMs
  );
  persist.npcIntervalMinMs = min;
  persist.npcIntervalMaxMs = max;
  const span = Math.max(1000, max - min);
  const delay = min + Math.random() * span;
  npcServerTimer = setTimeout(() => {
    const pair = NPC_MESSAGES[Math.floor(Math.random() * NPC_MESSAGES.length)];
    const m = {
      id: randomUUID(),
      channelId: 'main',
      authorId: 'npc',
      authorName: pair[0],
      text: pair[1],
      timestamp: Date.now(),
      isNPC: true,
    };
    messages.push(m);
    if (messages.length > MAX_MESSAGES) messages.splice(0, messages.length - MAX_MESSAGES);
    broadcast({ type: 'append', message: m });
    scheduleNpcServer();
  }, delay);
}

function broadcast(obj) {
  const packet = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (client.readyState === 1) client.send(packet);
  }
}

function patchAccount(userId, patch) {
  const prev = persist.accountPatches[userId] || {};
  persist.accountPatches[userId] = { ...prev, ...patch };
  savePersist();
}

function snapshotPayload() {
  return {
    type: 'snapshot',
    messages: [...messages],
    friendRequests: [...friendRequests],
    orders: [...orders],
    profileReactions: [...profileReactions],
    npcBot: {
      npcBotEnabled: persist.npcBotEnabled,
      npcIntervalMinMs: persist.npcIntervalMinMs,
      npcIntervalMaxMs: persist.npcIntervalMaxMs,
    },
    accountPatches: { ...persist.accountPatches },
  };
}

const wss = new WebSocketServer({ port: PORT });
console.log(`[chat-server] ws://127.0.0.1:${PORT} state=${STATE_PATH}`);

scheduleNpcServer();

wss.on('connection', (socket) => {
  socket.send(JSON.stringify(snapshotPayload()));

  socket.on('message', (buf) => {
    try {
      const parsed = JSON.parse(String(buf));

      if (parsed.type === 'message' && parsed.message && !parsed.message.isNPC) {
        const m = parsed.message;
        if (typeof m.id !== 'string') return;
        if (messages.some((x) => x.id === m.id)) return;
        messages.push(m);
        if (messages.length > MAX_MESSAGES) messages.splice(0, messages.length - MAX_MESSAGES);
        broadcast({ type: 'append', message: m });
        return;
      }

      if (parsed.type === 'friend-request-new' && parsed.request) {
        const r = parsed.request;
        if (typeof r.id !== 'string') return;
        if (friendRequests.some((x) => x.id === r.id)) return;
        if (
          friendRequests.some(
            (x) => x.fromId === r.fromId && x.toId === r.toId && x.status === 'pending'
          )
        ) {
          return;
        }
        friendRequests.push(r);
        if (friendRequests.length > MAX_FRIEND_REQUESTS) {
          friendRequests.splice(0, friendRequests.length - MAX_FRIEND_REQUESTS);
        }
        broadcast({ type: 'friend-request-append', request: r });
        return;
      }

      if (parsed.type === 'friend-request-update' && parsed.requestId && parsed.status) {
        const { requestId, status } = parsed;
        if (status !== 'accepted' && status !== 'rejected') return;
        const idx = friendRequests.findIndex((x) => x.id === requestId);
        if (idx >= 0) friendRequests[idx] = { ...friendRequests[idx], status };
        broadcast({ type: 'friend-request-status', requestId, status });
        return;
      }

      if (parsed.type === 'profile-reaction-new' && parsed.reaction) {
        const r = parsed.reaction;
        if (typeof r.id !== 'string') return;
        if (profileReactions.some((x) => x.id === r.id)) return;
        profileReactions.push(r);
        broadcast({ type: 'profile-reaction-append', reaction: r });
        return;
      }

      if (parsed.type === 'order-create' && parsed.order) {
        const o = parsed.order;
        if (typeof o.id !== 'string') return;
        if (orders.some((x) => x.id === o.id)) return;
        orders.push(o);
        broadcast({ type: 'order-append', order: o });
        return;
      }

      if (parsed.type === 'order-accept' && parsed.orderId && parsed.userId && parsed.userName && parsed.channelId) {
        const { orderId, userId, userName, channelId } = parsed;
        const idx = orders.findIndex((x) => x.id === orderId);
        if (idx >= 0) {
          orders[idx] = { ...orders[idx], acceptedById: userId, acceptedByName: userName, channelId };
        }
        broadcast({ type: 'order-accept', orderId, userId, userName, channelId });
        return;
      }

      if (parsed.type === 'order-complete' && parsed.orderId) {
        const { orderId, channelId } = parsed;
        const idx = orders.findIndex((x) => x.id === orderId);
        if (idx >= 0) orders.splice(idx, 1);
        if (typeof channelId === 'string') {
          for (let i = messages.length - 1; i >= 0; i--) {
            const m = messages[i];
            if (m && typeof m === 'object' && m.channelId === channelId) {
              messages.splice(i, 1);
            }
          }
        }
        broadcast({ type: 'order-complete', orderId, channelId });
        return;
      }

      if (parsed.type === 'npc-bot-config') {
        if (typeof parsed.npcBotEnabled === 'boolean') {
          persist.npcBotEnabled = parsed.npcBotEnabled;
        }
        if (
          typeof parsed.npcIntervalMinMs === 'number' ||
          typeof parsed.npcIntervalMaxMs === 'number'
        ) {
          const c = clampNpcIntervals(
            parsed.npcIntervalMinMs ?? persist.npcIntervalMinMs,
            parsed.npcIntervalMaxMs ?? persist.npcIntervalMaxMs
          );
          persist.npcIntervalMinMs = c.min;
          persist.npcIntervalMaxMs = c.max;
        }
        savePersist();
        clearNpcServer();
        scheduleNpcServer();
        broadcast({
          type: 'npc-bot-config',
          npcBotEnabled: persist.npcBotEnabled,
          npcIntervalMinMs: persist.npcIntervalMinMs,
          npcIntervalMaxMs: persist.npcIntervalMaxMs,
        });
        return;
      }

      if (parsed.type === 'account-moderation' && parsed.userId && parsed.patch && typeof parsed.patch === 'object') {
        patchAccount(parsed.userId, parsed.patch);
        broadcast({ type: 'account-moderation', userId: parsed.userId, patch: parsed.patch });
        return;
      }

      if (parsed.type === 'account-stats' && parsed.userId && typeof parsed.sentMessagesCount === 'number') {
        patchAccount(parsed.userId, {
          sentMessagesCount: parsed.sentMessagesCount,
        });
        broadcast({
          type: 'account-stats',
          userId: parsed.userId,
          sentMessagesCount: parsed.sentMessagesCount,
        });
        return;
      }

      if (parsed.type === 'friend-link-remove' && parsed.userId && parsed.friendId) {
        const [a, b] = [parsed.userId, parsed.friendId].sort();
        for (let i = friendRequests.length - 1; i >= 0; i--) {
          const r = friendRequests[i];
          if (!r || typeof r !== 'object') continue;
          const pair = [r.fromId, r.toId].sort();
          if (pair[0] === a && pair[1] === b && r.status === 'accepted') {
            friendRequests.splice(i, 1);
          }
        }
        broadcast({ type: 'friend-link-remove', userId: parsed.userId, friendId: parsed.friendId });
        return;
      }

      if (parsed.type === 'channel-clear' && typeof parsed.channelId === 'string') {
        const channelId = parsed.channelId;
        for (let i = messages.length - 1; i >= 0; i--) {
          const m = messages[i];
          if (m && typeof m === 'object' && m.channelId === channelId) {
            messages.splice(i, 1);
          }
        }
        broadcast({ type: 'channel-clear', channelId });
      }
    } catch {
      /* ignore */
    }
  });
});
