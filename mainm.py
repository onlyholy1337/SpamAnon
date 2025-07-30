import asyncio
import json
import os
from typing import Dict, List, Any, Optional

# --- Aiogram ---
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
# --- Telethon ---
from telethon import TelegramClient, events
from telethon.errors.rpcerrorlist import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError

# --- КОНФИГУРАЦИЯ ---
API_ID = 12345678  # Ваш API ID с my.telegram.org
API_HASH = 'u_api_hash'  # Ваш API HASH с my.telegram.org
BOT_TOKEN = 'TOKEN'  # Токен от @BotFather
ADMIN_ID = 12344535231  # ID пользователя, которому доступен бот

# --- Настройки ---
TARGET_BOT_USERNAME = '@tvoianon_bot'
DIALOG_LIMIT = 50  # внутренний лимит диалогов на одну сессию
DATA_DIR = "bot_data"
SESSIONS_DIR = os.path.join(DATA_DIR, "sessions")
DATA_FILE = os.path.join(DATA_DIR, "config.json")

# --- Глобальные переменные для управления состоянием ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Словари для хранения активных клиентов и задач
telethon_clients: Dict[str, TelegramClient] = {}
spam_tasks: Dict[str, asyncio.Task] = {}
orchestrator_task: Optional[asyncio.Task] = None
orchestrator_lock = asyncio.Lock()
dialog_lock = asyncio.Lock()


# --- Управление данными (загрузка/сохранение) ---
def load_data() -> Dict[str, Any]:
    if not os.path.exists(DATA_FILE): return {"sessions": {}, "messages": {}}
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {"sessions": {}, "messages": {}}


def save_data(data: Dict[str, Any]):
    with open(DATA_FILE, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4, ensure_ascii=False)


# --- FSM Состояния для Aiogram ---
class SessionStates(StatesGroup):
    enter_phone, enter_code, enter_password = State(), State(), State()


class MessageStates(StatesGroup):
    set_messages = State()
    set_all_messages = State()


# --- Логика воркера рассылки на Telethon (ФИНАЛЬНАЯ ВЕРСИЯ) ---
async def spam_worker(client: TelegramClient, session_name: str, messages_config: List[Dict[str, Any]]):
    done_event = asyncio.Event()
    script_lock = asyncio.Lock()

    @client.on(events.NewMessage(from_users=TARGET_BOT_USERNAME, incoming=True))
    async def message_handler(event: events.NewMessage.Event):
        nonlocal dialog_count, is_chatting

        text = event.raw_text
        text_lower = text.lower()
        print(f"[{session_name}] Получено: '{text[:80].replace(os.linesep, ' ')}...'")

        if text.startswith("😈 Вместе с вами общаются"):
            print(f"[{session_name}] Игнорирую системное сообщение о количестве пользователей.")
            return

        if "достигнут лимит диалогов за сутки" in text_lower:
            print(f"[{session_name}] Получен суточный лимит от бота. Завершаю сессию.")
            done_event.set()
            return

        end_dialog_phrases = ["собеседник закончил диалог", "вы закончили диалог", "поиск собеседника остановлен"]
        if any(phrase in text_lower for phrase in end_dialog_phrases):
            if is_chatting:
                print(f"[{session_name}] Диалог завершен. Ищу следующего...")
                is_chatting = False
                await asyncio.sleep(1)
                await client.send_message(TARGET_BOT_USERNAME, '/next')
            return

        if "собеседник найден" in text_lower and not is_chatting:
            async with script_lock:
                if is_chatting:
                    return

                is_chatting = True
                dialog_count += 1
                print(f"[{session_name}] Собеседник найден (Диалог #{dialog_count}). Отправляю сценарий...")

                try:
                    for msg_data in messages_config:
                        await asyncio.sleep(msg_data['delay'])
                        if not is_chatting:
                            print(f"[{session_name}] Сценарий прерван, диалог закончился во время ожидания.")
                            break
                        await event.respond(msg_data['text'])
                        print(f"[{session_name}] > '{msg_data['text']}'")

                    if is_chatting:
                        print(f"[{session_name}] Сценарий завершен. Успешных диалогов: {dialog_count}/{DIALOG_LIMIT}.")
                        await asyncio.sleep(1)
                        await event.respond('/stop')

                except Exception as e:
                    print(f"[{session_name}] Ошибка в цикле отправки сообщений: {e}.")
                    is_chatting = False
                    await event.respond('/next')

                finally:
                    if dialog_count >= DIALOG_LIMIT:
                        print(f"[{session_name}] Достигнут наш лимит в {DIALOG_LIMIT} диалогов.")
                        done_event.set()

    # --- Основная логика и жизненный цикл воркера ---
    try:
        if not client.is_connected(): await client.connect()
        if not await client.is_user_authorized():
            print(f"[{session_name}] Сессия не авторизована, воркер остановлен.")
            return

        dialog_count = 0
        is_chatting = False

        print(f"[{session_name}] Воркер запущен. Отправляю /stop и /next для сброса состояния...")
        await client.send_message(TARGET_BOT_USERNAME, '/stop')
        await asyncio.sleep(2)
        await client.send_message(TARGET_BOT_USERNAME, '/next')

        await done_event.wait()

    except Exception as e:
        print(f"[{session_name}] Критическая ошибка в работе воркера: {e}")
    finally:
        if client.is_connected():
            client.remove_event_handler(message_handler)
        print(f"[{session_name}] Воркер окончательно остановлен.")


# --- Логика Оркестратора ---
async def session_orchestrator(chat_id: int, message_id: int):
    global orchestrator_task
    print("[Оркестратор] Запущен в последовательном режиме.")
    data = load_data()
    sessions_to_run = list(data.get("sessions", {}).keys())

    if not sessions_to_run:
        print("[Оркестратор] Нет сессий для запуска.")
        await bot.send_message(chat_id, "Нет настроенных сессий для запуска.")
        return

    try:
        for session_name in sessions_to_run:
            print(f"\n========================================================")
            print(f"[Оркестратор] --- Начинаю работу с сессией: {session_name} ---")

            success = await _start_worker_task(session_name)
            if not success:
                print(f"[Оркестратор] Не удалось запустить сессию {session_name}, пропускаю.")
                continue

            worker_task = spam_tasks.get(session_name)
            if worker_task:
                await worker_task

            print(f"[Оркестратор] --- Сессия {session_name} завершила работу. Очищаю ресурсы. ---")
            await _stop_worker_task(session_name)
            print(f"========================================================\n")

        print("[Оркестратор] 🎉 Все сессии в очереди успешно обработаны по очереди.")

    except asyncio.CancelledError:
        print("[Оркестратор] 🛑 Главный цикл был отменен пользователем.")
    finally:
        orchestrator_task = None
        spam_tasks.clear()
        print("[Оркестратор] Завершил работу.")
        try:
            await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=get_main_control_keyboard()
            )
        except TelegramBadRequest:
            pass
        except Exception as e:
            print(f"[Оркестратор] Ошибка при обновлении клавиатуры: {e}")


# --- Вспомогательные функции для запуска/остановки ---
async def _start_worker_task(session_name: str) -> bool:
    data = load_data()
    messages_config = data.get("messages", {}).get(session_name)
    if not messages_config:
        print(f"[{session_name}] Невозможно запустить: не настроены сообщения.")
        return False

    try:
        session_path = os.path.join(SESSIONS_DIR, f"{session_name}.session")
        client = TelegramClient(session_path, API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            print(f"[{session_name}] Невозможно запустить: сессия не авторизована.")
            await client.disconnect()
            return False

        telethon_clients[session_name] = client
        worker_task = asyncio.create_task(spam_worker(client, session_name, messages_config))
        spam_tasks[session_name] = worker_task
        return True
    except Exception as e:
        print(f"[{session_name}] Ошибка при запуске воркера: {e}")
        return False


async def _stop_worker_task(session_name: str):
    print(f"Остановка воркера для сессии {session_name}...")
    task = spam_tasks.pop(session_name, None)
    if task and not task.done():
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    client = telethon_clients.pop(session_name, None)
    if client and client.is_connected():
        await client.disconnect()

    print(f"Ресурсы для сессии {session_name} очищены.")


# --- Клавиатуры ---
def get_main_control_keyboard() -> InlineKeyboardMarkup:
    is_running = orchestrator_task is not None and not orchestrator_task.done()
    start_stop_button = InlineKeyboardButton(text="❌ Остановить очередь",
                                             callback_data="stop_orchestrator") if is_running else InlineKeyboardButton(
        text="✅ Запустить очередь", callback_data="start_orchestrator")
    return InlineKeyboardMarkup(inline_keyboard=[[start_stop_button], [
        InlineKeyboardButton(text="🗂️ Управление сессиями", callback_data="manage_sessions")]])


def get_sessions_keyboard() -> InlineKeyboardMarkup:
    data = load_data()
    sessions = data.get("sessions", {}).keys()
    active_session = next((name for name, task in spam_tasks.items() if not task.done()), None)

    buttons = []
    if sessions:
        for name in sessions:
            status_emoji = "🟢" if name == active_session else "🔴"
            buttons.append([InlineKeyboardButton(text=f"{status_emoji} {name}", callback_data=f"session_menu_{name}")])

    buttons.append([InlineKeyboardButton(text="📝 Применить сценарий ко всем", callback_data="apply_to_all")])
    buttons.append([InlineKeyboardButton(text="➕ Добавить новую сессию", callback_data="add_session")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_session_menu_keyboard(session_name: str) -> InlineKeyboardMarkup:
    is_orchestrator_running = (orchestrator_task is not None and not orchestrator_task.done())

    active_spam_task = spam_tasks.get(session_name)
    is_this_session_running = (active_spam_task is not None and not active_spam_task.done())

    is_any_other_task_running = any(
        task for name, task in spam_tasks.items() if name != session_name and not task.done())
    is_any_process_running = is_orchestrator_running or is_this_session_running or is_any_other_task_running

    buttons = []

    if is_this_session_running:
        buttons.append(
            [InlineKeyboardButton(text="❌ Остановить эту сессию", callback_data=f"stop_single_{session_name}")])
    elif not is_any_process_running:
        buttons.append(
            [InlineKeyboardButton(text="▶️ Запустить эту сессию", callback_data=f"start_single_{session_name}")])

    buttons.extend([
        [InlineKeyboardButton(text="💬 Настроить сообщения", callback_data=f"setup_msg_{session_name}")],
        [InlineKeyboardButton(text="🗑️ Удалить сессию", callback_data=f"delete_session_{session_name}")],
        [InlineKeyboardButton(text="⬅️ Назад к списку сессий", callback_data="manage_sessions")]
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# --- Общая функция для отображения главного меню ---
async def show_main_menu(message: Message, edit: bool = False):
    text = "👋 Привет!\n\nЯ бот для управления рассылкой.\nИспользуйте меню для запуска очереди или настройки отдельных сессий."
    markup = get_main_control_keyboard()
    try:
        if edit:
            await message.edit_text(text, reply_markup=markup)
        else:
            await message.answer(text, reply_markup=markup)
    except TelegramBadRequest:
        pass


# --- Обработчики Aiogram ---
@router.message(CommandStart())
async def handle_start_cmd(message: Message):
    if message.from_user.id != ADMIN_ID: return
    await show_main_menu(message)


@router.callback_query(F.data == "main_menu")
async def handle_main_menu_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    await show_main_menu(callback.message, edit=True)
    await callback.answer()


@router.callback_query(F.data == "manage_sessions")
async def cb_manage_sessions(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    await state.clear()
    try:
        await callback.message.edit_text(
            "🗂️ Управление сессиями:\n\nВыберите сессию для настройки или примените сценарий ко всем. 🟢 - активна.",
            reply_markup=get_sessions_keyboard()
        )
    except TelegramBadRequest:
        pass
    await callback.answer()


# --- Процесс добавления сессии ---
@router.callback_query(F.data == "add_session")
async def cb_add_session(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    await state.set_state(SessionStates.enter_phone)
    try:
        await callback.message.edit_text(
            "📱 Введите номер телефона для новой сессии в международном формате.\n*(например, +79123456789)*")
    except TelegramBadRequest:
        pass
    await callback.answer()


@router.message(SessionStates.enter_phone)
async def process_phone(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    phone = message.text
    session_path = os.path.join(SESSIONS_DIR, f"{phone}.session")
    client = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await client.connect()
        if await client.is_user_authorized():
            data = load_data()
            data.setdefault("sessions", {})[phone] = "active"
            save_data(data)
            await message.answer(f"✅ Сессия для {phone} уже существует и успешно подключена!",
                                 reply_markup=get_sessions_keyboard())
            await state.clear()
            await client.disconnect()
            return

        sent_code = await client.send_code_request(phone)
        await state.update_data(phone=phone, hash=sent_code.phone_code_hash)
        await state.set_state(SessionStates.enter_code)
        telethon_clients[f"temp_{message.from_user.id}"] = client
        await message.answer("✉️ Код подтверждения был отправлен в ваш аккаунт Telegram. Пожалуйста, введите его:")

    except FloodWaitError as e:
        await message.answer(f"❌ Слишком много попыток. Подождите {e.seconds} секунд.")
        await state.clear()
        if client.is_connected(): await client.disconnect()
    except Exception as e:
        await message.answer(f"❌ Произошла ошибка: {e}\n\nПопробуйте снова.")
        await state.clear()
        if client.is_connected(): await client.disconnect()


@router.message(SessionStates.enter_code)
async def process_code(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    code = message.text
    user_data = await state.get_data()
    phone, code_hash = user_data['phone'], user_data['hash']
    client = telethon_clients.get(f"temp_{message.from_user.id}")
    try:
        await client.sign_in(phone, code, phone_code_hash=code_hash)
        data = load_data()
        data.setdefault("sessions", {})[phone] = "active"
        save_data(data)
        await message.answer(f"✅ Сессия для {phone} успешно добавлена!", reply_markup=get_sessions_keyboard())
        await state.clear()
    except SessionPasswordNeededError:
        await state.set_state(SessionStates.enter_password)
        await message.answer(
            "🔑 Этот аккаунт защищен двухфакторной аутентификацией. Введите ваш пароль (облачный пароль):")
    except PhoneCodeInvalidError:
        await message.answer("❌ Неверный код. Пожалуйста, попробуйте добавить сессию заново.",
                             reply_markup=get_sessions_keyboard())
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка при входе: {e}", reply_markup=get_sessions_keyboard())
        await state.clear()
    finally:
        if await state.get_state() != SessionStates.enter_password:
            if client and client.is_connected(): await client.disconnect()
            if f"temp_{message.from_user.id}" in telethon_clients: del telethon_clients[f"temp_{message.from_user.id}"]


@router.message(SessionStates.enter_password)
async def process_password(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    password = message.text
    user_data = await state.get_data()
    phone = user_data['phone']
    client = telethon_clients.get(f"temp_{message.from_user.id}")
    try:
        await client.sign_in(password=password)
        data = load_data()
        data.setdefault("sessions", {})[phone] = "active"
        save_data(data)
        await message.answer(f"✅ Сессия для {phone} успешно добавлена!", reply_markup=get_sessions_keyboard())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}. Неверный пароль или другая проблема.",
                             reply_markup=get_sessions_keyboard())
    finally:
        if client and client.is_connected(): await client.disconnect()
        if f"temp_{message.from_user.id}" in telethon_clients: del telethon_clients[f"temp_{message.from_user.id}"]
        await state.clear()


# --- Управление сессиями и сообщениями ---
@router.callback_query(F.data.startswith("session_menu_"))
async def cb_session_menu(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    session_name = callback.data.split("_")[-1]
    try:
        await callback.message.edit_text(f"⚙️ Управление сессией: <b>{session_name}</b>",
                                         reply_markup=get_session_menu_keyboard(session_name))
    except TelegramBadRequest:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("delete_session_"))
async def cb_delete_session(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    session_name = callback.data.split("_")[-1]
    is_any_task_running = (orchestrator_task is not None and not orchestrator_task.done()) or any(
        t for t in spam_tasks.values() if not t.done())
    if is_any_task_running:
        await callback.answer("❌ Сначала остановите все запущенные процессы.", show_alert=True)
        return

    data = load_data()
    if session_name in data["sessions"]: del data["sessions"][session_name]
    if session_name in data.get("messages", {}): del data["messages"][session_name]
    save_data(data)
    session_file = os.path.join(SESSIONS_DIR, f"{session_name}.session")
    if os.path.exists(session_file): os.remove(session_file)
    await callback.answer("Сессия успешно удалена!", show_alert=True)
    await cb_manage_sessions(callback, state)


@router.callback_query(F.data.startswith("setup_msg_"))
async def cb_setup_messages(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    session_name = callback.data.split("_")[-1]
    await state.update_data(session_name=session_name)
    await state.set_state(MessageStates.set_messages)
    data = load_data()
    messages_list = data.get("messages", {}).get(session_name, [])
    current_messages_text = "\n".join(f"• {msg['delay']} сек. — «{msg['text']}»" for msg in
                                      messages_list) if messages_list else "<i>(пока не настроены)</i>"
    try:
        await callback.message.edit_text(
            f"📝 Настройка сообщений для {session_name}\n\n"
            "Отправьте мне сценарий в формате:\n`задержка | Текст`\n"
            f"<b>Текущий сценарий:</b>\n{current_messages_text}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"session_menu_{session_name}")]]))
    except TelegramBadRequest:
        pass


@router.message(MessageStates.set_messages)
async def process_messages_input(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    user_data = await state.get_data()
    session_name = user_data['session_name']
    lines, messages_config, errors = message.text.strip().split('\n'), [], []
    for i, line in enumerate(lines):
        parts = line.split('|', 1)
        if len(parts) != 2:
            errors.append(f"Строка {i + 1}: неверный формат.")
            continue
        try:
            delay, text = int(parts[0].strip()), parts[1].strip()
            if not text:
                errors.append(f"Строка {i + 1}: текст не может быть пустым.")
            else:
                messages_config.append({"delay": delay, "text": text})
        except ValueError:
            errors.append(f"Строка {i + 1}: задержка должна быть числом.")
    if errors:
        await message.answer("❌ Ошибки в сценарии:\n- " + "\n- ".join(errors) + "\n\nПопробуйте снова.")
        return
    data = load_data()
    data.setdefault("messages", {})[session_name] = messages_config
    save_data(data)
    await state.clear()
    await message.answer(f"✅ Сценарий для <b>{session_name}</b> сохранен.",
                         reply_markup=get_session_menu_keyboard(session_name))


# --- Новые обработчики для применения сценария ко всем ---
@router.callback_query(F.data == "apply_to_all")
async def cb_apply_to_all(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    await state.set_state(MessageStates.set_all_messages)
    try:
        await callback.message.edit_text(
            "📜 Применить сценарий ко всем\n\n"
            "Отправьте сценарий, который будет применен ко ВСЕМ сессиям.\n"
            "Формат:\n`задержка | Текст сообщения 1`\n`задержка | Текст сообщения 2`",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_sessions")]])
        )
    except TelegramBadRequest:
        pass
    await callback.answer()


@router.message(MessageStates.set_all_messages)
async def process_all_messages_input(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    lines, messages_config, errors = message.text.strip().split('\n'), [], []
    for i, line in enumerate(lines):
        parts = line.split('|', 1)
        if len(parts) != 2:
            errors.append(f"Строка {i + 1}: неверный формат.")
            continue
        try:
            delay, text = int(parts[0].strip()), parts[1].strip()
            if not text:
                errors.append(f"Строка {i + 1}: текст не может быть пустым.")
            else:
                messages_config.append({"delay": delay, "text": text})
        except ValueError:
            errors.append(f"Строка {i + 1}: задержка должна быть числом.")
    if errors:
        await message.answer("❌ Ошибки в сценарии:\n- " + "\n- ".join(errors) + "\n\nПопробуйте снова.")
        return

    data = load_data()
    session_names = list(data.get("sessions", {}).keys())
    if not session_names:
        await message.answer("Нет добавленных сессий для применения сценария.")
        await state.clear()
        return

    for session_name in session_names:
        data.setdefault("messages", {})[session_name] = messages_config
    save_data(data)
    await state.clear()
    await message.answer(f"✅ Сценарий успешно применен к {len(session_names)} сессиям.",
                         reply_markup=get_sessions_keyboard())


# --- Управление Оркестратором и отдельными сессиями ---
@router.callback_query(F.data == "start_orchestrator")
async def cb_start_orchestrator(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)

    async with orchestrator_lock:
        global orchestrator_task
        if any(t for t in spam_tasks.values() if not t.done()):
            await callback.answer("❌ Сначала остановите активную сессию.", show_alert=True)
            return
        if orchestrator_task is not None and not orchestrator_task.done():
            await callback.answer("❌ Очередь уже запущена!", show_alert=True)
            return

        await callback.answer("✅ Запускаю очередь сессий...", show_alert=True)
        orchestrator_task = asyncio.create_task(
            session_orchestrator(callback.message.chat.id, callback.message.message_id))
        try:
            await callback.message.edit_reply_markup(reply_markup=get_main_control_keyboard())
        except TelegramBadRequest:
            pass


@router.callback_query(F.data == "stop_orchestrator")
async def cb_stop_orchestrator(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    global orchestrator_task
    if orchestrator_task is not None and not orchestrator_task.done():
        await callback.answer("❌ Останавливаю очередь...", show_alert=True)
        orchestrator_task.cancel()
        for task in list(spam_tasks.values()): task.cancel()
        orchestrator_task = None
        try:
            await callback.message.edit_reply_markup(reply_markup=get_main_control_keyboard())
        except TelegramBadRequest:
            pass
    else:
        await callback.answer("❌ Очередь не запущена.", show_alert=True)


@router.callback_query(F.data.startswith("start_single_"))
async def cb_start_single_session(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    session_name = callback.data.split("_")[-1]
    is_any_task_running = (orchestrator_task is not None and not orchestrator_task.done()) or any(
        t for t in spam_tasks.values() if not t.done())
    if is_any_task_running:
        await callback.answer("❌ Другой процесс уже запущен. Остановите его.", show_alert=True)
        return

    await callback.answer(f"▶️ Запускаю сессию {session_name}...", show_alert=True)
    success = await _start_worker_task(session_name)
    if not success:
        await callback.answer(f"❌ Не удалось запустить сессию {session_name}. Проверьте сообщения или авторизацию.",
                              show_alert=True)

    try:
        await callback.message.edit_reply_markup(reply_markup=get_session_menu_keyboard(session_name))
    except TelegramBadRequest:
        pass


@router.callback_query(F.data.startswith("stop_single_"))
async def cb_stop_single_session(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return await callback.answer("Доступ запрещен", show_alert=True)
    session_name = callback.data.split("_")[-1]
    await callback.answer(f"⏹️ Останавливаю сессию {session_name}...", show_alert=True)
    await _stop_worker_task(session_name)
    try:
        await callback.message.edit_reply_markup(reply_markup=get_session_menu_keyboard(session_name))
    except TelegramBadRequest:
        pass


async def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    print("Бот запускается...")
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Бот остановлен.")
