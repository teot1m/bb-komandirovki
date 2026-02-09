const SPREADSHEET_ID = PropertiesService.getScriptProperties().getProperty('SPREADSHEET_ID');
const TELEGRAM_BOT_TOKEN = PropertiesService.getScriptProperties().getProperty('TELEGRAM_BOT_TOKEN');
const DRIVE_FOLDER_ID = PropertiesService.getScriptProperties().getProperty('DRIVE_FOLDER_ID');
const DRIVE_FOLDER_NAME = PropertiesService.getScriptProperties().getProperty('DRIVE_FOLDER_NAME');
const GDRIVE_SA_CLIENT_EMAIL = PropertiesService.getScriptProperties().getProperty('GDRIVE_SA_CLIENT_EMAIL');
const GDRIVE_SA_PRIVATE_KEY = PropertiesService.getScriptProperties().getProperty('GDRIVE_SA_PRIVATE_KEY');
const GDRIVE_SA_TOKEN_URI = "https://oauth2.googleapis.com/token";
const SHEET_LOGS = "Logs";
const SHEET_SETTINGS = "Settings";
const SHEET_EXPENSES = "Expenses";

const COL = {
  USER_ID: 1, USER_NAME: 2, REQ_ID: 3, CREATED_AT: 4,
  COMPANY: 5, DEPT: 6, PURPOSE: 7, DATE_START: 8, DATE_END: 9,
  PEOPLE: 10,
  PER_DIEM_NAME: 11, PER_DIEM_RATE: 12, DAILY_TOTAL: 13,
  PLAN_ITEMS_JSON: 14, PLAN_ITEMS_TOTAL: 15,
  ADDITIONAL_ITEMS_JSON: 16, ADDITIONAL_ITEMS_TOTAL: 17,
  PAYMENT: 18, PAYMENT_CARD: 19,
  STATUS: 20, APPROVER: 21,
  UPDATED_AT: 22, COMPLETED_AT: 23,
  LOG_JSON: 24
};

// Время жизни кэша (в секундах)
const CACHE_TTL = 3600; // 1 час

function doGet(e) {
  const action = e.parameter.action;
  const userId = e.parameter.userId;
  let result = {};

  try {
    if (action === 'getUserInfo') {
      result = getUserInfoWithCache(userId);
    } else if (action === 'getDepartments') {
      result = getDepartmentsWithCache();
    } else if (action === 'getUserRequests') {
      result = getUserRequests(userId);
    } else if (action === 'getPendingRequests') {
      result = getPendingRequests(userId);
    } else if (action === 'getAdminRequests') {
      result = getAdminRequests(userId);
    } else if (action === 'getDailyRates') {
      result = getDailyRates();
    } else if (action === 'getTripExpenseOptions') {
      result = getTripExpenseOptions();
    } else if (action === 'getPendingExpenses') {
      result = getPendingExpenses(userId);
    } else if (action === 'getPendingExpensesGrouped') {
      result = getPendingExpensesGrouped(userId);
    } else if (action === 'getExpensesByReqId') {
      result = getExpensesByReqId(e.parameter.reqId, userId);
    }
  } catch (err) {
    result = { error: err.toString() };
  }

  return ContentService.createTextOutput(JSON.stringify(result)).setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  let result = {};
  const logs = [];
  const log = (msg) => { try { logs.push(msg); } catch (e) {} };
  // Блокировка от одновременных запросов (Race Condition)
  const lock = LockService.getScriptLock();

  try {
    // Ждем 10 секунд, если кто-то другой пишет в таблицу прямо сейчас
    if (lock.tryLock(10000)) {
      const params = JSON.parse(e.postData.contents);

      if (params.action === 'createRequest') {
        result = { status: 'success', message: createRequest(params.data) };
      } else if (params.action === 'approveRequest') {
        result = { status: 'success', message: approveRequest(params.rowId, params.approver, params.decision, params.approverId) };
      } else if (params.action === 'updateAndApproveRequest') {
        result = { status: 'success', message: updateAndApproveRequest(params.rowId, params.approver, params.approverId, params.data) };
      } else if (params.action === 'requestClarification') {
        result = { status: 'success', message: requestClarification(params.rowId, params.adminId, params.adminName, params.question) };
      } else if (params.action === 'submitClarificationAnswer') {
        result = { status: 'success', message: submitClarificationAnswer(params.rowId, params.userId, params.answer) };
      } else if (params.action === 'submitExpenses') {
        result = submitExpenses(params.rowId, params.userId, params.userName, params.expenses, log);
      } else if (params.action === 'decideExpense') {
        result = { status: 'success', message: decideExpense(params.expenseId, params.approver, params.decision) };
      } else if (params.action === 'decideExpensesBatch') {
        result = { status: 'success', message: decideExpensesBatch(params.expenseIds, params.approver, params.decision, params.expenseItems) };
      } else if (params.action === 'completeTrip') {
        result = { status: 'success', message: completeTrip(params.rowId, params.userId) };
      } else if (params.action === 'clearCache') {
        result = { status: 'success', message: clearCache(params.userId) };
      }
    } else {
      result = { status: 'error', message: "Сервер зайнятий. Спробуйте ще раз." };
    }
  } catch (err) {
    result = { status: 'error', message: err.toString() };
  } finally {
    lock.releaseLock();
  }

  if (logs.length) result.debug = logs;
  return ContentService.createTextOutput(JSON.stringify(result)).setMimeType(ContentService.MimeType.JSON);
}

// --- КЭШИРОВАНИЕ И ДАННЫЕ ---
function getSS() { return SpreadsheetApp.openById(SPREADSHEET_ID); }
function getLogsSheet() {
  const sheet = getSS().getSheetByName(SHEET_LOGS);
  ensureLogsHeaders(sheet);
  return sheet;
}

function getExpensesSheet() {
  const ss = getSS();
  let sheet = ss.getSheetByName(SHEET_EXPENSES);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_EXPENSES);
  }
  ensureExpensesHeaders(sheet);
  return sheet;
}

// Получение юзера с кэшем
function getUserInfoWithCache(userId) {
  const cache = CacheService.getScriptCache();
  const cacheKey = "user_v4_" + userId;
  const cached = cache.get(cacheKey);

  if (cached) return JSON.parse(cached);

  const info = getUserInfo(userId);
  if (info) {
    cache.put(cacheKey, JSON.stringify(info), CACHE_TTL);
    return info;
  }
  return { error: "User not found" };
}

// Получение отделов с кэшем
function getDepartmentsWithCache() {
  const cache = CacheService.getScriptCache();
  const cached = cache.get("depts_v1");

  if (cached) return JSON.parse(cached);

  const depts = getDepartments();
  cache.put("depts_v1", JSON.stringify(depts), CACHE_TTL);
  return depts;
}

function clearCache(userId) {
  const cache = CacheService.getScriptCache();
  const keys = ["depts_v1", "rates_v1", "opts_v1"];
  if (userId) keys.push("user_v4_" + userId);
  cache.removeAll(keys);
  return "ok";
}

// --- ПРЯМОЙ ДОСТУП К ТАБЛИЦЕ ---

function getUserInfo(userId) {
  const ss = getSS();
  const sheet = ss.getSheetByName(SHEET_SETTINGS);
  const data = sheet.getDataRange().getValues();
  data.shift();
  const row = data.find(r => String(r[0]) === String(userId));
  if (!row) return null;
  return {
    id: String(row[0]),
    name: row[1],
    role: row[2],
    company: String(row[3]),
    cards: row[4] ? String(row[4]) : ""
  };
}

function getDepartments() {
  const ss = getSS();
  const sheet = ss.getSheetByName(SHEET_SETTINGS);
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return [];
  return sheet.getRange("H2:H" + lastRow).getValues().flat().filter(String);
}

function getDailyRates() {
  const cache = CacheService.getScriptCache();
  const cached = cache.get("rates_v2");
  if (cached) return JSON.parse(cached);
  const ss = getSS();
  const sheet = ss.getSheetByName(SHEET_SETTINGS);
  const rows = sheet.getRange("K3:M16").getValues();
  const rates = rows
    .filter(r => String(r[0]).trim())
    .map(r => ({
      name: String(r[0]).trim(),
      rateShort: Number(r[1]) || 0,
      rateLong: Number(r[2]) || 0
    }));
  cache.put("rates_v2", JSON.stringify(rates), CACHE_TTL);
  return rates;
}

function getTripExpenseOptions() {
  const cache = CacheService.getScriptCache();
  const cached = cache.get("opts_v1");
  if (cached) return JSON.parse(cached);
  const ss = getSS();
  const sheet = ss.getSheetByName(SHEET_SETTINGS);
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return [];
  const rows = sheet.getRange("I2:I" + lastRow).getValues().flat().filter(String);
  const opts = rows.map(v => String(v).trim()).filter(Boolean);
  cache.put("opts_v1", JSON.stringify(opts), CACHE_TTL);
  return opts;
}

function createRequest(form) {
  const ss = getSS();
  // Проверка прав без кэша (для надежности при записи)
  const userInfo = getUserInfo(form.userId);
  if (!userInfo) throw new Error("Користувача не знайдено");
  const allowed = String(userInfo.company).split(',').map(c => c.trim());
  if (!allowed.includes(form.company)) throw new Error("Немає доступу до компанії");
  if (form.paymentMethod === "Карта" && !form.paymentCard) throw new Error("Вкажіть карту для зарахування");

  const diffDays = getTripDays(form.dateStart, form.dateEnd);
  const rateInfo = resolvePerDiemRates(form.perDiemName, diffDays, form.perDiemRate);
  const dailyTotal = calculateDailyTotalTiers(form.dateStart, form.dateEnd, form.peopleCount, rateInfo.rateShort, rateInfo.rateLong);
  const planItems = form.planItems || [];
  const planItemsTotal = planItems.reduce((sum, i) => sum + (Number(i.amount) || 0), 0);
  const ts = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd.MM.yyyy HH:mm");
  const uniq = new Date().getTime().toString();

  const logArr = [{
    type: "запит",
    userId: form.userId,
    userName: form.userName,
    date: ts,
    text: "Створено заявку"
  }];
  if (form.comment) {
    logArr.push({
      type: "коментар",
      userId: form.userId,
      userName: form.userName,
      date: ts,
      text: form.comment
    });
  }
  const log = JSON.stringify(logArr);

  const rowData = [
    form.userId, form.userName, uniq, ts, form.company, form.department, form.purpose,
    "'" + form.dateStart, "'" + form.dateEnd,
    form.peopleCount,
    form.perDiemName || "", rateInfo.rateLong, dailyTotal,
    JSON.stringify(planItems), planItemsTotal,
    "", 0,
    form.paymentMethod, form.paymentCard || "",
    "Нова", "",
    "", "", log
  ];
  ss.getSheetByName(SHEET_LOGS).appendRow(rowData);
  return "✅ Заявку створено!";
}

function getUserRequests(userId) {
  const sheet = getLogsSheet();

  // ОПТИМИЗАЦИЯ: Читаем только последние 200 строк, если таблица огромная
  // Это значительно ускоряет работу
  const lastRow = sheet.getLastRow();
  const totalRows = lastRow - 1;
  const limit = 200; // Читаем максимум 200 последних строк
  const startRow = Math.max(2, lastRow - limit + 1);
  const numRows = lastRow - startRow + 1;

  if (numRows < 1) return [];

  const numCols = Math.max(sheet.getLastColumn(), COL.LOG_JSON);
  const data = sheet.getRange(startRow, 1, numRows, numCols).getValues();

  // Фильтруем, переворачиваем и отдаем только последние 50 на клиент
  const approvedMap = buildApprovedExpensesMap();
  return data.map(mapRowToObj)
    .filter(i => String(i.userId) === String(userId))
    .map(i => addApprovedExpenses(i, approvedMap))
    .reverse()
    .slice(0, 50);
}

function getPendingRequests(userId) {
  const userInfo = getUserInfo(userId); // Здесь кэш не нужен, нужна актуальность
  if (!userInfo || userInfo.role !== 'Адмін') return [];
  const allowed = String(userInfo.company).split(',').map(c => c.trim());

  const sheet = getLogsSheet();
  const data = sheet.getDataRange().getValues();
  data.shift();

  const approvedMap = buildApprovedExpensesMap();
  return data.map(mapRowToObj)
    .filter(i => (i.status === "Нова" || i.status === "Уточнено" || i.status === "Потребує уточнення") && allowed.includes(i.company))
    .map(i => addApprovedExpenses(i, approvedMap))
    .reverse();
}

function getAdminRequests(userId) {
  const userInfo = getUserInfo(userId);
  if (!userInfo || userInfo.role !== 'Адмін') return [];
  const allowed = String(userInfo.company).split(',').map(c => c.trim());
  const sheet = getLogsSheet();
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return [];
  const limit = 300;
  const startRow = Math.max(2, lastRow - limit + 1);
  const numRows = lastRow - startRow + 1;
  const numCols = Math.max(sheet.getLastColumn(), COL.LOG_JSON);
  const data = sheet.getRange(startRow, 1, numRows, numCols).getValues();
  const approvedMap = buildApprovedExpensesMap();
  return data.map(mapRowToObj)
    .filter(i => allowed.includes(i.company))
    .map(i => addApprovedExpenses(i, approvedMap))
    .reverse();
}

function approveRequest(reqId, approver, decision, approverId) {
  const sheet = getLogsSheet();
  // Оптимизация поиска: читаем только колонку ID
  const ids = sheet.getRange("C:C").getValues().flat();
  const rowIndex = ids.findIndex(id => String(id) === String(reqId));
  if (rowIndex === -1) throw new Error("Заявку не знайдено");

  const r = rowIndex + 1;
  const currentStatus = sheet.getRange(r, COL.STATUS).getValue();

  if (currentStatus === "Завершено") throw new Error("Заявку завершено");
  if (currentStatus !== "Нова" && currentStatus !== "Уточнено") throw new Error("Вже оброблено (" + currentStatus + ")");

  const status = decision === 'approve' ? 'Погоджено' : 'Відхилено';
  sheet.getRange(r, COL.STATUS).setValue(status);
  sheet.getRange(r, COL.APPROVER).setValue(approver);
  sheet.getRange(r, COL.UPDATED_AT).setValue(nowTs());
  appendLogEntry(sheet, r, {
    type: "рішення",
    userId: approverId,
    userName: approver,
    date: nowTs(),
    text: status
  });

  const req = mapRowToObj(sheet.getRange(r, 1, 1, Math.max(sheet.getLastColumn(), COL.LOG_JSON)).getValues()[0]);
  const msg = buildApprovalMessage(req, status, req.editSummary, req.adminComment);
  const tg = sendTelegramMessage(req.userId, msg);
  if (!tg.ok) appendLogEntry(sheet, r, { type: "telegram_error", userId: approverId, userName: approver, date: nowTs(), text: tg.error });
  return decision === 'approve' ? "✅ Погоджено!" : "❌ Відхилено.";
}

function mapRowToObj(r) {
  const logJson = r[23] || "";
  const logEntries = parseLogJson(logJson);
  return {
    userId: r[0], userName: r[1], reqId: r[2], created: r[3], company: r[4], dept: r[5],
    purpose: r[6], dStart: String(r[7]).replace("'", ""), dEnd: String(r[8]).replace("'", ""),
    people: r[9],
    perDiemName: r[10] || "", perDiemRate: r[11] || 0, dailyTotal: r[12] || 0,
    planItemsJson: r[13] || "", planItemsTotal: r[14] || 0,
    additionalItemsJson: r[15] || "", additionalItemsTotal: r[16] || 0,
    payment: r[17], paymentCard: r[18] || "",
    status: r[19], approver: r[20] || "",
    updatedAt: r[21] || "", completedAt: r[22] || "",
    logJson: logJson,
    logEntries: logEntries,
    editSummary: findLastLogText(logEntries, "зміни"),
    adminComment: findLastLogText(logEntries, "коментар_адмін"),
    userComment: findLastLogText(logEntries, "коментар"),
    clarifyQuestion: normalizeClarify(findLastLogText(logEntries, "запит")),
    clarifyAnswer: findLastLogText(logEntries, "відповідь")
  };
}

function normalizeClarify(text) {
  const t = String(text || "").trim();
  if (!t || t === "Створено заявку") return "";
  return t;
}

function updateAndApproveRequest(reqId, approver, approverId, data) {
  if (!data || !data.adminComment || !String(data.adminComment).trim()) {
    throw new Error("Потрібен коментар адміністратора");
  }
  const sheet = getLogsSheet();
  const ids = sheet.getRange("C:C").getValues().flat();
  const rowIndex = ids.findIndex(id => String(id) === String(reqId));
  if (rowIndex === -1) throw new Error("Заявку не знайдено");
  const r = rowIndex + 1;

  const current = mapRowToObj(sheet.getRange(r, 1, 1, Math.max(sheet.getLastColumn(), COL.LOG_JSON)).getValues()[0]);
  if (current.status === "Завершено") throw new Error("Заявку завершено");
  if (current.status !== "Нова" && current.status !== "Уточнено") throw new Error("Вже оброблено (" + current.status + ")");

  const updated = {
    company: data.company || current.company,
    dept: data.department || current.dept,
    purpose: data.purpose || current.purpose,
    dStart: data.dateStart || current.dStart,
    dEnd: data.dateEnd || current.dEnd,
    people: data.peopleCount || current.people,
    payment: data.paymentMethod || current.payment,
    card: data.paymentCard || current.paymentCard,
    perDiemName: data.perDiemName || current.perDiemName,
    perDiemRate: Number(data.perDiemRate) || Number(current.perDiemRate) || 0,
    planItemsJson: data.planItemsJson || current.planItemsJson,
    planItemsTotal: Number(data.planItemsTotal) || Number(current.planItemsTotal) || 0
  };

  if (updated.payment !== "Карта") updated.card = "";
  if (updated.payment === "Карта" && !updated.card) {
    throw new Error("Вкажіть карту для зарахування");
  }

  const days = getTripDays(updated.dStart, updated.dEnd);
  const rateInfo = resolvePerDiemRates(updated.perDiemName, days, updated.perDiemRate);
  updated.perDiemRate = rateInfo.rateLong;
  const dailyTotal = calculateDailyTotalTiers(updated.dStart, updated.dEnd, updated.people, rateInfo.rateShort, rateInfo.rateLong);
  const summary = buildEditSummary(current, updated, dailyTotal);

  const row = [
    updated.company, updated.dept, updated.purpose,
    "'" + updated.dStart, "'" + updated.dEnd,
    updated.people,
    updated.perDiemName, updated.perDiemRate, dailyTotal,
    updated.planItemsJson, updated.planItemsTotal,
    current.additionalItemsJson || "", current.additionalItemsTotal || 0,
    updated.payment, updated.card,
    "Погоджено", approver,
    nowTs(), current.completedAt || "", current.logJson || ""
  ];

  sheet.getRange(r, COL.COMPANY, 1, 1).setValue(row[0]);
  sheet.getRange(r, COL.DEPT, 1, 1).setValue(row[1]);
  sheet.getRange(r, COL.PURPOSE, 1, 1).setValue(row[2]);
  sheet.getRange(r, COL.DATE_START, 1, 1).setValue(row[3]);
  sheet.getRange(r, COL.DATE_END, 1, 1).setValue(row[4]);
  sheet.getRange(r, COL.PEOPLE, 1, 1).setValue(row[5]);
  sheet.getRange(r, COL.PER_DIEM_NAME, 1, 1).setValue(row[6]);
  sheet.getRange(r, COL.PER_DIEM_RATE, 1, 1).setValue(row[7]);
  sheet.getRange(r, COL.DAILY_TOTAL, 1, 1).setValue(row[8]);
  sheet.getRange(r, COL.PLAN_ITEMS_JSON, 1, 1).setValue(row[9]);
  sheet.getRange(r, COL.PLAN_ITEMS_TOTAL, 1, 1).setValue(row[10]);
  sheet.getRange(r, COL.ADDITIONAL_ITEMS_JSON, 1, 1).setValue(row[11]);
  sheet.getRange(r, COL.ADDITIONAL_ITEMS_TOTAL, 1, 1).setValue(row[12]);
  sheet.getRange(r, COL.PAYMENT, 1, 1).setValue(row[13]);
  sheet.getRange(r, COL.PAYMENT_CARD, 1, 1).setValue(row[14]);
  sheet.getRange(r, COL.STATUS, 1, 1).setValue(row[15]);
  sheet.getRange(r, COL.APPROVER, 1, 1).setValue(row[16]);
  sheet.getRange(r, COL.UPDATED_AT, 1, 1).setValue(row[17]);
  sheet.getRange(r, COL.COMPLETED_AT, 1, 1).setValue(row[18]);
  sheet.getRange(r, COL.LOG_JSON, 1, 1).setValue(row[19]);

  appendLogEntry(sheet, r, {
    type: "зміни",
    userId: approverId,
    userName: approver,
    date: nowTs(),
    text: summary
  });
  appendLogEntry(sheet, r, {
    type: "коментар_адмін",
    userId: approverId,
    userName: approver,
    date: nowTs(),
    text: data.adminComment
  });
  appendLogEntry(sheet, r, {
    type: "рішення",
    userId: approverId,
    userName: approver,
    date: nowTs(),
    text: "Погоджено"
  });

  const req = mapRowToObj(sheet.getRange(r, 1, 1, Math.max(sheet.getLastColumn(), COL.LOG_JSON)).getValues()[0]);
  const msg = buildApprovalMessage(req, "Погоджено", summary, data.adminComment);
  sendTelegramMessage(req.userId, msg);
  return "✅ Погоджено з можливими змінами.";
}

function requestClarification(reqId, adminId, adminName, question) {
  if (!question) throw new Error("Потрібне уточнення");
  const sheet = getLogsSheet();
  const ids = sheet.getRange("C:C").getValues().flat();
  const rowIndex = ids.findIndex(id => String(id) === String(reqId));
  if (rowIndex === -1) throw new Error("Заявку не знайдено");
  const r = rowIndex + 1;
  const currentStatus = sheet.getRange(r, COL.STATUS).getValue();
  if (currentStatus === "Завершено") throw new Error("Заявку завершено");
  if (currentStatus !== "Нова" && currentStatus !== "Уточнено") throw new Error("Вже оброблено (" + currentStatus + ")");

  sheet.getRange(r, COL.STATUS).setValue("Потребує уточнення");
  sheet.getRange(r, COL.UPDATED_AT).setValue(nowTs());
  appendLogEntry(sheet, r, {
    type: "запит",
    userId: adminId,
    userName: adminName,
    date: nowTs(),
    text: question
  });

  const req = mapRowToObj(sheet.getRange(r, 1, 1, Math.max(sheet.getLastColumn(), COL.LOG_JSON)).getValues()[0]);
  const tg = sendTelegramMessage(req.userId, "📝 Потрібне уточнення по заявці \"" + req.purpose + "\":\n" + question);
  if (!tg.ok) appendLogEntry(sheet, r, { type: "telegram_error", userId: adminId, userName: adminName, date: nowTs(), text: tg.error });
  return "✅ Уточнення відправлено.";
}

function submitClarificationAnswer(reqId, userId, answer) {
  if (!answer) throw new Error("Вкажіть відповідь");
  const sheet = getLogsSheet();
  const ids = sheet.getRange("C:C").getValues().flat();
  const rowIndex = ids.findIndex(id => String(id) === String(reqId));
  if (rowIndex === -1) throw new Error("Заявку не знайдено");
  const r = rowIndex + 1;

  const row = sheet.getRange(r, 1, 1, Math.max(sheet.getLastColumn(), COL.LOG_JSON)).getValues()[0];
  const req = mapRowToObj(row);
  if (String(req.userId) !== String(userId)) throw new Error("Немає доступу");
  if (req.status === "Завершено") throw new Error("Заявку завершено");
  if (req.status !== "Потребує уточнення") throw new Error("Статус не дозволяє відповідь");

  sheet.getRange(r, COL.STATUS).setValue("Уточнено");
  sheet.getRange(r, COL.UPDATED_AT).setValue(nowTs());
  appendLogEntry(sheet, r, {
    type: "відповідь",
    userId: userId,
    userName: req.userName,
    date: nowTs(),
    text: answer
  });

  const text = "✅ Отримано уточнення по заявці \"" + req.purpose + "\" від " + req.userName + ":\n" + answer;
  const lastAsk = findLastLogEntry(req.logEntries || [], "запит");
  if (lastAsk && lastAsk.userId) {
    const tg = sendTelegramMessage(lastAsk.userId, text);
    if (!tg.ok) notifyAdminsForCompany(req.company, text);
  } else {
    notifyAdminsForCompany(req.company, text);
  }
  return "✅ Відповідь відправлено.";
}

function normId(v) {
  return String(v == null ? "" : v).trim().replace(/^'+/, "").replace(/\s+/g, "");
}

function withReqLock(reqId, fn) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    return fn();
  } finally {
    try { lock.releaseLock(); } catch (e) {}
  }
}

function submitExpenses(reqId, userId, userName, expenses, logFn) {
  const log = logFn || function () {};
  log("submitExpenses start. reqId=" + reqId + " userId=" + userId + " userName=" + userName);
  if (!expenses || expenses.length === 0) throw new Error("Немає витрат");

  return withReqLock(reqId, function () {
    const logsSheet = getLogsSheet();
    const ids = logsSheet.getRange("C:C").getValues().flat();
    const rowIndex = ids.findIndex(id => String(id) === String(reqId));
    if (rowIndex === -1) throw new Error("Заявку не знайдено");
    const r = rowIndex + 1;
    const req = mapRowToObj(logsSheet.getRange(r, 1, 1, Math.max(logsSheet.getLastColumn(), COL.LOG_JSON)).getValues()[0]);
    if (normId(req.userId) !== normId(userId)) throw new Error("Немає доступу");
    if (req.status !== "Погоджено") throw new Error("Витрати можна додати лише після погодження");

  const expenseSheet = getExpensesSheet();
  const batchId = new Date().getTime().toString();
  const ts = nowTs();
  const useServiceAccount = Boolean(GDRIVE_SA_CLIENT_EMAIL && GDRIVE_SA_PRIVATE_KEY);
  const driveInfo = getDriveFolderSafe();
  const folder = driveInfo.folder;
  const folderId = DRIVE_FOLDER_ID || (folder ? folder.getId() : "");
  log("Drive mode: serviceAccount=" + useServiceAccount + " folderId=" + folderId + " name=" + (DRIVE_FOLDER_NAME || "") + " err=" + (driveInfo.error || ""));
  if (folder) {
    try {
      log("Drive folder url=" + folder.getUrl() + " name=" + folder.getName());
    } catch (e) {
      log("Drive folder meta error: " + e);
    }
  }
  let fileIndex = getNextFileIndex(userName, reqId, folder);
  let filesSkipped = false;
  let folderError = driveInfo.error || "";

  expenses.forEach(exp => {
    const files = exp.files || [];
    const urls = [];
    log("Expense item: name=" + exp.name + " amount=" + exp.amount + " files=" + files.length + " link=" + (exp.link || ""));
    if (files.length > 0 && !folderId) {
      filesSkipped = true;
      log("FolderId missing, files skipped.");
    } else if (useServiceAccount && files.length > 0) {
      files.forEach(f => {
        const ext = (f.name && f.name.indexOf('.') !== -1) ? f.name.substring(f.name.lastIndexOf('.')) : '';
        const fileName = sanitizeFileName(userName) + "_" + reqId + "_" + fileIndex + ext;
        fileIndex += 1;
        const bytes = Utilities.base64Decode(f.data);
        try {
          log("SA upload: name=" + fileName + " mime=" + f.mime + " size=" + bytes.length);
          const fileInfo = uploadFileToDriveServiceAccount(folderId, fileName, f.mime, bytes, log);
          if (fileInfo && fileInfo.webViewLink) urls.push(fileInfo.webViewLink);
        } catch (e) {
          filesSkipped = true;
          log("SA upload error: " + e);
        }
      });
    } else if (folder) {
      files.forEach(f => {
        const ext = (f.name && f.name.indexOf('.') !== -1) ? f.name.substring(f.name.lastIndexOf('.')) : '';
        const fileName = sanitizeFileName(userName) + "_" + reqId + "_" + fileIndex + ext;
        fileIndex += 1;
        const bytes = Utilities.base64Decode(f.data);
        const blob = Utilities.newBlob(bytes, f.mime, fileName);
        try {
          log("Creating file: name=" + fileName + " mime=" + f.mime + " size=" + bytes.length);
          const file = folder.createFile(blob);
          log("File created: id=" + file.getId() + " url=" + file.getUrl());
          urls.push(file.getUrl());
        } catch (e) {
          filesSkipped = true;
          log("Drive createFile error: " + e);
        }
      });
    } else if (files.length > 0) {
      filesSkipped = true;
      log("Folder missing, files skipped.");
    }

    const expenseId = batchId + "_" + Math.floor(Math.random() * 100000);
    expenseSheet.appendRow([
      expenseId, reqId, userId, userName, ts,
      exp.name, exp.amount, exp.description || "", exp.link || "",
      urls.join(", "), "Нова", "", "", req.company
    ]);
  });

  // pending count no longer stored in logs

  notifyAdminsForCompany(req.company, "🧾 Нові витрати по заявці \"" + req.purpose + "\" від " + userName + ". Потрібне затвердження.");
  log("submitExpenses done. filesSkipped=" + filesSkipped);
  if (filesSkipped) {
    const extra = folderError ? (" " + folderError) : "";
    return { status: "success", message: "✅ Витрати надіслано, але файли не завантажені (немає доступу до Drive або папку не знайдено)." + extra };
  }
  return { status: "success", message: "✅ Витрати надіслано на затвердження." };
  });
}

function decideExpense(expenseId, approver, decision) {
  const sheet = getExpensesSheet();
  const ids = sheet.getRange("A:A").getValues().flat();
  const rowIndex = ids.findIndex(id => String(id) === String(expenseId));
  if (rowIndex === -1) throw new Error("Витрату не знайдено");
  const r = rowIndex + 1;
  const status = decision === 'approve' ? 'Погоджено' : 'Відхилено';
  // amount update not supported in single mode
  sheet.getRange(r, 11).setValue(status);
  sheet.getRange(r, 12).setValue(approver);
  sheet.getRange(r, 13).setValue(nowTs());

  const row = sheet.getRange(r, 1, 1, 14).getValues()[0];
  const reqId = row[1];
  const userId = row[2];
  const name = row[5];
  const amount = row[6];
  const req = getRequestById(reqId);
  const purpose = req ? req.purpose : reqId;
  const msg = buildExpensesDecisionMessage(purpose, decision, [{ name: name, amount: amount }]);
  sendTelegramMessage(userId, msg);

  updateApprovedExpensesSummary(reqId);
  return status === 'Погоджено' ? "✅ Витрату погоджено." : "❌ Витрату відхилено.";
}

function decideExpensesBatch(expenseIds, approver, decision, expenseItems) {
  if (!expenseIds || expenseIds.length === 0) throw new Error("Немає витрат");
  const sheet = getExpensesSheet();
  const ids = sheet.getRange("A:A").getValues().flat();
  const status = decision === 'approve' ? 'Погоджено' : 'Відхилено';
  const now = nowTs();
  const updates = {};
  if (expenseItems && expenseItems.length) {
    expenseItems.forEach(it => {
      if (!it || !it.expenseId) return;
      updates[String(it.expenseId)] = it.amount;
    });
  }

  const touchedReqs = {};
  const notifyMap = {};
  expenseIds.forEach(expenseId => {
    const rowIndex = ids.findIndex(id => String(id) === String(expenseId));
    if (rowIndex === -1) return;
    const r = rowIndex + 1;
    const updAmount = updates[String(expenseId)];
    if (updAmount !== undefined && updAmount !== null && updAmount !== "") {
      sheet.getRange(r, 7).setValue(Number(updAmount) || 0);
    }
    sheet.getRange(r, 11).setValue(status);
    sheet.getRange(r, 12).setValue(approver);
    sheet.getRange(r, 13).setValue(now);

    const row = sheet.getRange(r, 1, 1, 14).getValues()[0];
    const reqId = row[1];
    const userId = row[2];
    const name = row[5];
    const amount = row[6];
    const req = getRequestById(reqId);
    const purpose = req ? req.purpose : reqId;
    if (!notifyMap[reqId]) notifyMap[reqId] = { userId: userId, purpose: purpose, items: [] };
    notifyMap[reqId].items.push({ name: name, amount: amount });
    touchedReqs[reqId] = true;
  });

  Object.keys(notifyMap).forEach(reqId => {
    const info = notifyMap[reqId];
    const msg = buildExpensesDecisionMessage(info.purpose, decision, info.items);
    sendTelegramMessage(info.userId, msg);
  });

  Object.keys(touchedReqs).forEach(reqId => updateApprovedExpensesSummary(reqId));
  return status === 'Погоджено' ? "✅ Витрати погоджено." : "❌ Витрати відхилено.";
}

function getPendingExpenses(userId) {
  const userInfo = getUserInfo(userId);
  if (!userInfo || userInfo.role !== 'Адмін') return [];
  const allowed = String(userInfo.company).split(',').map(c => c.trim());
  const sheet = getExpensesSheet();
  const data = sheet.getDataRange().getValues();
  data.shift();
  return data.filter(r => r[10] === "Нова" && allowed.includes(r[13]))
    .map(r => ({
      expenseId: r[0], reqId: r[1], userId: r[2], userName: r[3], created: r[4],
      name: r[5], amount: r[6], description: r[7], link: r[8], fileUrls: r[9],
      status: r[10], company: r[13]
    })).reverse();
}

function getPendingExpensesGrouped(userId) {
  const userInfo = getUserInfo(userId);
  if (!userInfo || userInfo.role !== 'Адмін') return [];
  const allowed = String(userInfo.company).split(',').map(c => c.trim());

  const reqMap = buildReqMap();
  const sheet = getExpensesSheet();
  const data = sheet.getDataRange().getValues();
  data.shift();

  const groups = {};
  data.forEach(r => {
    if (r[10] !== "Нова") return;
    if (!allowed.includes(r[13])) return;
    const reqId = r[1];
    if (!groups[reqId]) {
      const req = reqMap[reqId] || {};
      groups[reqId] = {
        reqId: reqId,
        company: r[13],
        userId: r[2],
        userName: r[3],
        purpose: req.purpose || "",
        dStart: req.dStart || "",
        dEnd: req.dEnd || "",
        items: []
      };
    }
    groups[reqId].items.push({
      expenseId: r[0],
      name: r[5],
      amount: r[6],
      description: r[7],
      link: r[8],
      fileUrls: r[9],
      created: r[4]
    });
  });
  return Object.keys(groups).map(k => groups[k]).reverse();
}

function getExpensesByReqId(reqId, userId) {
  if (!reqId) return [];
  const userInfo = getUserInfo(userId);
  if (!userInfo) return [];
  const req = getRequestById(reqId);
  if (!req) return [];
  const allowed = String(userInfo.company).split(',').map(c => c.trim());
  const isAdmin = userInfo.role === 'Адмін' && allowed.includes(req.company);
  const isOwner = String(req.userId) === String(userId);
  if (!isAdmin && !isOwner) return [];

  const sheet = getExpensesSheet();
  const data = sheet.getDataRange().getValues();
  data.shift();
  return data.filter(r => String(r[1]) === String(reqId))
    .map(r => ({
      expenseId: r[0], reqId: r[1], userId: r[2], userName: r[3], created: r[4],
      name: r[5], amount: r[6], description: r[7], link: r[8], fileUrls: r[9],
      status: r[10], approver: r[11], decidedAt: r[12], company: r[13]
    }));
}

function getPendingExpensesCount(reqId) {
  const sheet = getExpensesSheet();
  const data = sheet.getDataRange().getValues();
  data.shift();
  return data.filter(r => String(r[1]) === String(reqId) && r[10] === "Нова").length;
}

function updatePendingExpensesCount(reqId) {
  const logsSheet = getLogsSheet();
  const ids = logsSheet.getRange("C:C").getValues().flat();
  const rowIndex = ids.findIndex(id => String(id) === String(reqId));
  if (rowIndex === -1) return;
  const r = rowIndex + 1;
  // no-op: pending count no longer stored
}

function getRequestById(reqId) {
  const sheet = getLogsSheet();
  const ids = sheet.getRange("C:C").getValues().flat();
  const rowIndex = ids.findIndex(id => String(id) === String(reqId));
  if (rowIndex === -1) return null;
  const r = rowIndex + 1;
  return mapRowToObj(sheet.getRange(r, 1, 1, Math.max(sheet.getLastColumn(), COL.LOG_JSON)).getValues()[0]);
}

function buildReqMap() {
  const sheet = getLogsSheet();
  const data = sheet.getDataRange().getValues();
  data.shift();
  const map = {};
  data.forEach(r => {
    const obj = mapRowToObj(r);
    map[obj.reqId] = obj;
  });
  return map;
}

function updateApprovedExpensesSummary(reqId) {
  const logsSheet = getLogsSheet();
  const ids = logsSheet.getRange("C:C").getValues().flat();
  const rowIndex = ids.findIndex(id => String(id) === String(reqId));
  if (rowIndex === -1) return;
  const r = rowIndex + 1;

  const approvedMap = buildApprovedExpensesMap();
  const entry = approvedMap[reqId] || { sum: 0, items: [] };
  logsSheet.getRange(r, COL.ADDITIONAL_ITEMS_JSON).setValue(JSON.stringify(entry.items));
  logsSheet.getRange(r, COL.ADDITIONAL_ITEMS_TOTAL).setValue(entry.sum);
}

function buildApprovedExpensesMap() {
  const sheet = getExpensesSheet();
  const data = sheet.getDataRange().getValues();
  data.shift();
  const map = {};
  data.forEach(r => {
    const reqId = r[1];
    const status = r[10];
    const amount = Number(r[6]) || 0;
    if (status === "Погоджено") {
      if (!map[reqId]) map[reqId] = { sum: 0, items: [] };
      map[reqId].sum += amount;
      map[reqId].items.push({
        name: r[5],
        amount: amount,
        description: r[7],
        link: r[8],
        fileUrls: r[9],
        approver: r[11],
        decidedAt: r[12]
      });
    }
  });
  return map;
}

function addApprovedExpenses(req, approvedMap) {
  const entry = approvedMap[req.reqId] || { sum: 0, items: [] };
  req.additionalItemsTotal = entry.sum;
  req.additionalItemsJson = JSON.stringify(entry.items);
  return req;
}

function hasUnresolvedAdditionalExpenses(reqId) {
  // Only "Погоджено" is final for closing. Rejected expenses still block completion.
  const FINAL = { "Погоджено": true };
  const sheet = getExpensesSheet();
  const data = sheet.getDataRange().getValues();
  data.shift();
  for (var i = 0; i < data.length; i++) {
    const r = data[i];
    if (String(r[1]) !== String(reqId)) continue; // reqId
    const status = r[10];
    if (!FINAL[String(status || "")]) return true;
  }
  return false;
}

function completeTrip(reqId, userId) {
  return withReqLock(reqId, function () {
    const sheet = getLogsSheet();
    const ids = sheet.getRange("C:C").getValues().flat();
    const rowIndex = ids.findIndex(id => String(id) === String(reqId));
    if (rowIndex === -1) throw new Error("Заявку не знайдено");
    const r = rowIndex + 1;
    const row = sheet.getRange(r, 1, 1, Math.max(sheet.getLastColumn(), COL.COMPLETED_AT)).getValues()[0];
    const req = mapRowToObj(row);
    if (normId(req.userId) !== normId(userId)) throw new Error("Немає доступу");
    if (req.status !== "Погоджено") throw new Error("Завершити можна лише погоджену заявку");

    // Block closing when there are additional expenses not yet decided (anything except final statuses)
    if (hasUnresolvedAdditionalExpenses(reqId)) {
      throw new Error("У вас есть несогласованные дополнительные расходы. Дождитесь решения руководителя перед закрытием.");
    }

    sheet.getRange(r, COL.STATUS).setValue("Завершено");
    sheet.getRange(r, COL.COMPLETED_AT).setValue(nowTs());
    appendLogEntry(sheet, r, {
      type: "завершено",
      userId: userId,
      userName: req.userName,
      date: nowTs(),
      text: "Завершено"
    });
    return "✅ Відрядження завершено.";
  });
}

function calculateDailyTotal(dStart, dEnd, people, rate) {
  const d1 = new Date(dStart);
  const d2 = new Date(dEnd);
  const diffDays = Math.ceil(Math.abs(d2 - d1) / (86400000)) + 1;
  const r = Number(rate) || 0;
  return diffDays * r * (parseInt(people) || 1);
}

function getTripDays(dStart, dEnd) {
  const d1 = new Date(dStart);
  const d2 = new Date(dEnd);
  return Math.ceil(Math.abs(d2 - d1) / (86400000)) + 1;
}

function resolvePerDiemRates(name, days, fallbackRate) {
  const rates = getDailyRates();
  const item = rates.find(r => String(r.name) === String(name));
  if (item) {
    return {
      rateShort: Number(item.rateShort) || 0,
      rateLong: Number(item.rateLong) || 0
    };
  }
  const fallback = Number(fallbackRate) || 0;
  return { rateShort: fallback, rateLong: fallback };
}

function calculateDailyTotalTiers(dStart, dEnd, people, rateShort, rateLong) {
  const d1 = new Date(dStart);
  const d2 = new Date(dEnd);
  const diffDays = Math.ceil(Math.abs(d2 - d1) / (86400000)) + 1;
  const shortRate = Number(rateShort) || 0;
  const longRate = Number(rateLong) || 0;
  const p = parseInt(people) || 1;
  const firstDays = Math.min(diffDays, 3);
  const restDays = Math.max(diffDays - 3, 0);
  return (firstDays * shortRate + restDays * longRate) * p;
}

function buildEditSummary(current, updated, newDaily) {
  const changes = [];
  if (current.company !== updated.company) changes.push("Компанія: " + current.company + " → " + updated.company);
  if (current.dept !== updated.dept) changes.push("Відділ: " + current.dept + " → " + updated.dept);
  if (current.purpose !== updated.purpose) changes.push("Мета: " + current.purpose + " → " + updated.purpose);
  if (current.dStart !== updated.dStart) changes.push("Дата початку: " + current.dStart + " → " + updated.dStart);
  if (current.dEnd !== updated.dEnd) changes.push("Дата кінця: " + current.dEnd + " → " + updated.dEnd);
  if (String(current.people) !== String(updated.people)) changes.push("Людей: " + current.people + " → " + updated.people);
  if (String(current.payment) !== String(updated.payment)) changes.push("Оплата: " + current.payment + " → " + updated.payment);
  if (String(current.paymentCard || "") !== String(updated.card || "")) changes.push("Карта: " + (current.paymentCard || "-") + " → " + (updated.card || "-"));
  if (String(current.perDiemRate || "") !== String(updated.perDiemRate || "")) changes.push("Добові: " + current.perDiemRate + " → " + updated.perDiemRate);
  if (String(current.dailyTotal) !== String(newDaily)) changes.push("Добові: " + current.dailyTotal + " → " + newDaily);
  return changes.length ? changes.join("; ") : "";
}

function buildApprovalMessage(req, status, summary, adminComment) {
  const total = (parseFloat(req.dailyTotal) || 0) + (parseFloat(req.planItemsTotal) || 0);
  const approvedExtra = parseFloat(req.additionalItemsTotal || 0) || 0;
  const totalAll = total + approvedExtra;
  const icon = status === "Відхилено" ? "❌" : "✅";
  const purpose = escapeTelegramText(req.purpose);
  const dStart = formatDateShort(req.dStart);
  const dEnd = formatDateShort(req.dEnd);
  let text = icon + " *Заявка \"" + purpose + "\" розглянута.*" +
    "\n*Статус:* " + escapeTelegramText(status) +
    "\n*Мета:* " + purpose +
    "\n*Дати:* " + dStart + " — " + dEnd +
    "\n*Сума:* " + totalAll + " ₴";
  if (summary) {
    text += "\n*Зміни:*\n" + formatSummaryLines(summary);
  }
  if (adminComment) text += "\n*Коментар:* " + escapeTelegramText(adminComment);
  return text;
}

function buildExpensesDecisionMessage(purpose, decision, items) {
  const icon = decision === "approve" ? "✅" : "❌";
  const title = decision === "approve" ? "Затверджено витрати" : "Відхилено витрати";
  const lines = (items || []).map(i => "• " + escapeTelegramText(i.name) + " — " + (Number(i.amount) || 0) + " ₴");
  let text = icon + " *" + title + " по заявці \"" + escapeTelegramText(purpose) + "\"*";
  if (lines.length) text += "\n" + lines.join("\n");
  return text;
}

function formatSummaryLines(summary) {
  return String(summary || "")
    .split(";")
    .map(s => s.trim())
    .filter(Boolean)
    .map(s => escapeTelegramText(s))
    .join("\n");
}

function formatDateShort(value) {
  const s = String(value || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const parts = s.split("-");
    return parts[2] + "." + parts[1] + "." + parts[0];
  }
  return s;
}

function escapeTelegramText(text) {
  return String(text || "").replace(/([_*\\[\\]()])/g, '\\$1');
}

function notifyAdminsForCompany(company, text) {
  const ss = getSS();
  const sheet = ss.getSheetByName(SHEET_SETTINGS);
  const data = sheet.getDataRange().getValues();
  data.shift();
  const failed = [];
  data.forEach(r => {
    const role = r[2];
    const id = r[0];
    const companies = String(r[3]).split(',').map(c => c.trim());
    if (role === "Адмін" && companies.includes(company)) {
      const tg = sendTelegramMessage(id, text);
      if (!tg.ok) failed.push(id);
    }
  });
  return { failed: failed };
}

function sendTelegramMessage(chatId, text) {
  if (!TELEGRAM_BOT_TOKEN || !chatId) {
    Logger.log("Telegram skipped. Token or chatId missing.");
    return { ok: false, error: "token_or_chat_missing" };
  }
  const url = "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage";
  const payload = {
    chat_id: String(chatId),
    text: text,
    parse_mode: "Markdown",
    disable_web_page_preview: true
  };
  try {
    const resp = UrlFetchApp.fetch(url, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });
    const code = resp.getResponseCode();
    const body = resp.getContentText();
    if (code !== 200) {
      Logger.log("Telegram response code: " + code + " body: " + body);
      return { ok: false, error: "telegram_" + code, body: body };
    }
    return { ok: true };
  } catch (e) {
    Logger.log("Telegram error: " + e);
    return { ok: false, error: "exception", body: String(e) };
  }
}

function nowTs() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd.MM.yyyy HH:mm");
}

function sanitizeFileName(name) {
  return String(name || "user").replace(/[^\w\-]+/g, "_");
}

function getNextFileIndex(userName, reqId, folder) {
  if (!folder) return 1;
  const prefix = sanitizeFileName(userName) + "_" + reqId + "_";
  // Считаем только файлы с нужным префиксом
  const files2 = folder.getFiles();
  let matched = 0;
  while (files2.hasNext()) {
    const f = files2.next();
    if (String(f.getName()).indexOf(prefix) === 0) matched += 1;
  }
  return matched + 1;
}

// Запросить все необходимые разрешения одним запуском
function authorizeAll() {
  SpreadsheetApp.openById(SPREADSHEET_ID).getSheets();
  try {
    if (DRIVE_FOLDER_ID) {
      DriveApp.getFolderById(DRIVE_FOLDER_ID).getName();
    } else if (DRIVE_FOLDER_NAME) {
      getFolderByNameOnly(DRIVE_FOLDER_NAME).getName();
    } else {
      DriveApp.getRootFolder().getName();
    }
  } catch (e) {
    Logger.log("Drive access error: " + e);
  }
  if (TELEGRAM_BOT_TOKEN) {
    UrlFetchApp.fetch("https://api.telegram.org", { muteHttpExceptions: true });
  }
  return "OK";
}

function getOrCreateFolderByName(name) {
  const folders = DriveApp.getFoldersByName(name);
  if (folders.hasNext()) return folders.next();
  return DriveApp.createFolder(name);
}

function getDriveFolderSafe() {
  const nameRaw = DRIVE_FOLDER_NAME ? String(DRIVE_FOLDER_NAME).trim() : "";
  const idRaw = DRIVE_FOLDER_ID ? String(DRIVE_FOLDER_ID).trim() : "";
  const maybeIdFromName = nameRaw && nameRaw.length >= 20 && nameRaw.indexOf(" ") === -1;

  if (idRaw || maybeIdFromName) {
    try {
      const id = idRaw || nameRaw;
      Logger.log("Drive lookup by ID: " + id);
      return { folder: DriveApp.getFolderById(id) };
    } catch (e) {
      Logger.log("Drive folder by ID error: " + e);
      return { folder: null, error: "Перевірте DRIVE_FOLDER_ID." };
    }
  }
  if (!nameRaw) return { folder: null, error: "Не вказано DRIVE_FOLDER_NAME або DRIVE_FOLDER_ID." };
  try {
    Logger.log("Drive lookup by name: " + nameRaw);
    const folder = getFolderByNameOnly(nameRaw);
    if (!folder) return { folder: null, error: "Папку не знайдено за назвою." };
    return { folder: folder };
  } catch (e) {
    Logger.log("Drive folder error: " + e);
    return { folder: null, error: "Немає доступу до папки." };
  }
}

function getFolderByNameOnly(name) {
  const folders = DriveApp.getFoldersByName(name);
  if (folders.hasNext()) return folders.next();
  return null;
}

function parseLogJson(raw) {
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    return [];
  }
}

function findLastLogText(entries, type) {
  if (!entries || !entries.length) return "";
  for (let i = entries.length - 1; i >= 0; i--) {
    if (entries[i].type === type) return entries[i].text || "";
  }
  return "";
}

function findLastLogEntry(entries, type) {
  if (!entries || !entries.length) return null;
  for (let i = entries.length - 1; i >= 0; i--) {
    if (entries[i].type === type) return entries[i];
  }
  return null;
}

function appendLogEntry(sheet, rowIndex, entry) {
  const cell = sheet.getRange(rowIndex, COL.LOG_JSON).getValue();
  const arr = parseLogJson(cell);
  arr.push(entry);
  sheet.getRange(rowIndex, COL.LOG_JSON).setValue(JSON.stringify(arr));
}

function uploadFileToDriveServiceAccount(folderId, name, mimeType, bytes, logFn) {
  const log = logFn || function () {};
  const token = getServiceAccountAccessToken();
  if (!token) throw new Error("Service Account token error");

  const metadata = { name: name, parents: [folderId] };
  const boundary = "-------314159265358979323846";
  const delimiter = "\r\n--" + boundary + "\r\n";
  const closeDelim = "\r\n--" + boundary + "--";

  const multipartBody =
    delimiter +
    "Content-Type: application/json; charset=UTF-8\r\n\r\n" +
    JSON.stringify(metadata) +
    delimiter +
    "Content-Type: " + (mimeType || "application/octet-stream") + "\r\n" +
    "Content-Transfer-Encoding: base64\r\n\r\n" +
    Utilities.base64Encode(bytes) +
    closeDelim;

  const uploadUrl = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink&supportsAllDrives=true";
  const resp = UrlFetchApp.fetch(uploadUrl, {
    method: "post",
    contentType: "multipart/related; boundary=" + boundary,
    headers: { Authorization: "Bearer " + token },
    payload: multipartBody,
    muteHttpExceptions: true
  });
  const code = resp.getResponseCode();
  const body = resp.getContentText();
  log("SA upload response code=" + code + " body=" + body);
  if (code !== 200) throw new Error("Drive upload failed: " + code);

  const file = JSON.parse(body);
  try {
    setDriveFilePublic(file.id, token, log);
  } catch (e) {
    log("Set public permission error: " + e);
  }
  return file;
}

function setDriveFilePublic(fileId, token, logFn) {
  const log = logFn || function () {};
  const url = "https://www.googleapis.com/drive/v3/files/" + fileId + "/permissions?supportsAllDrives=true";
  const payload = { role: "reader", type: "anyone" };
  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  log("Set public permission: code=" + resp.getResponseCode() + " body=" + resp.getContentText());
}

function getServiceAccountAccessToken() {
  if (!GDRIVE_SA_CLIENT_EMAIL || !GDRIVE_SA_PRIVATE_KEY) return null;
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const claim = {
    iss: GDRIVE_SA_CLIENT_EMAIL,
    scope: "https://www.googleapis.com/auth/drive",
    aud: GDRIVE_SA_TOKEN_URI,
    exp: now + 3600,
    iat: now
  };
  const headerB64 = base64UrlEncode(JSON.stringify(header));
  const claimB64 = base64UrlEncode(JSON.stringify(claim));
  const unsignedToken = headerB64 + "." + claimB64;
  const signature = Utilities.computeRsaSha256Signature(unsignedToken, GDRIVE_SA_PRIVATE_KEY);
  const signedToken = unsignedToken + "." + base64UrlEncode(signature);

  const resp = UrlFetchApp.fetch(GDRIVE_SA_TOKEN_URI, {
    method: "post",
    payload: {
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: signedToken
    },
    muteHttpExceptions: true
  });
  const code = resp.getResponseCode();
  const body = resp.getContentText();
  if (code !== 200) throw new Error("SA token error: " + body);
  const data = JSON.parse(body);
  return data.access_token;
}

function base64UrlEncode(input) {
  const bytes = (input instanceof Array) ? input : Utilities.newBlob(input).getBytes();
  return Utilities.base64EncodeWebSafe(bytes).replace(/=+$/, "");
}

function getLogsHeaders() {
  return [
    "UserId",
    "Користувач",
    "ReqId",
    "Створено",
    "Компанія",
    "Відділ",
    "Мета",
    "Дата початку",
    "Дата кінця",
    "Людей",
    "Ставка добових (назва)",
    "Ставка добових (грн)",
    "Сума добових",
    "Планові витрати (JSON)",
    "Сума планових",
    "Додаткові витрати (JSON)",
    "Сума додаткових",
    "Оплата",
    "Карта",
    "Статус",
    "Погодив",
    "Оновлено",
    "Завершено",
    "Лог (JSON)"
  ];
}

function getExpensesHeaders() {
  return [
    "ExpenseId", "ReqId", "UserId", "UserName", "CreatedAt",
    "Name", "Amount", "Description", "Link", "FileUrls",
    "Status", "Approver", "DecidedAt", "Company"
  ];
}

function ensureLogsHeaders(sheet) {
  if (!sheet) return;
  const headers = getLogsHeaders();
  const lastCol = sheet.getLastColumn();
  const firstRow = sheet.getRange(1, 1, 1, Math.max(lastCol, headers.length)).getValues()[0];
  const needsUpdate = firstRow.join("|").trim() !== headers.join("|");
  if (needsUpdate) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}

function ensureExpensesHeaders(sheet) {
  if (!sheet) return;
  const headers = getExpensesHeaders();
  const lastCol = sheet.getLastColumn();
  const firstRow = sheet.getRange(1, 1, 1, Math.max(lastCol, headers.length)).getValues()[0];
  const needsUpdate = firstRow.join("|").trim() !== headers.join("|");
  if (needsUpdate) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}
