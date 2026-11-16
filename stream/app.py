
import streamlit as st
import os
import queue
import threading
import time

from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

from dispatcher import (
    run_dispatcher,
    publish_d1_test_message
)

from driver import run_driver


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="Rapido Food Order Dispatch",
    page_icon="🚕",
    layout="wide"
)

load_dotenv()

AMQP_URL = os.getenv("RABBITMQ_URL")


DRIVERS = {
    "Ravi": "bangalore.koramangala.*",
    "Priya": "bangalore.indiranagar.*",
    "Arjun": "bangalore.*.ride",
    "Meena": "bangalore.*.food",
    "Karthik": "chennai.tnagar.food",
    "Control": "bangalore.#"
}


# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------

if "log_queue" not in st.session_state:
    st.session_state.log_queue = queue.Queue()

if "logs" not in st.session_state:
    st.session_state.logs = []

if "threads" not in st.session_state:
    st.session_state.threads = []

if "stop_event" not in st.session_state:
    st.session_state.stop_event = threading.Event()

if "started" not in st.session_state:
    st.session_state.started = False


# --------------------------------------------------
# LOG MANAGEMENT
# --------------------------------------------------

def collect_logs():

    q = st.session_state.log_queue
    logs = st.session_state.logs

    while True:

        try:
            category, message = q.get_nowait()

            logs.append({
                "component": category,
                "message": message,
                "time": time.strftime("%H:%M:%S")
            })

        except queue.Empty:
            break

    # Retain recent logs
    st.session_state.logs = logs[-500:]


collect_logs()


# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def workers_alive():

    return any(
        thread.is_alive()
        for thread in st.session_state.threads
    )


def start_system(selected_drivers, bad_every):

    if not AMQP_URL:
        st.error("RABBITMQ_URL is missing in your .env file.")
        return

    if workers_alive():
        st.warning("The system is already running.")
        return

    st.session_state.stop_event = threading.Event()

    st.session_state.log_queue = queue.Queue()

    st.session_state.logs = []

    stop_event = st.session_state.stop_event
    log_queue = st.session_state.log_queue

    threads = []

    # Start selected driver consumers first
    for name in selected_drivers:

        pattern = DRIVERS[name]

        t = threading.Thread(
            target=run_driver,
            args=(
                AMQP_URL,
                name,
                pattern,
                stop_event,
                log_queue
            ),
            daemon=True
        )

        threads.append(t)
        t.start()

    # Give consumers time to declare and bind their queues
    time.sleep(1)

    # Start dispatcher
    dispatcher_thread = threading.Thread(
        target=run_dispatcher,
        args=(
            AMQP_URL,
            stop_event,
            log_queue,
            bad_every
        ),
        daemon=True
    )

    threads.append(dispatcher_thread)
    dispatcher_thread.start()

    st.session_state.threads = threads
    st.session_state.started = True


def stop_system():

    st.session_state.stop_event.set()
    st.session_state.started = False


# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.title("🚕 Rapido Food Order Dispatch System")

st.markdown(
    """
    **Data Engineering | RabbitMQ Topic Exchange**

    A real-time food and ride request routing system
    using RabbitMQ, Python and Streamlit.
    """
)

st.divider()


# --------------------------------------------------
# SIDEBAR CONTROLS
# --------------------------------------------------

st.sidebar.title("⚙️ System Controls")

st.sidebar.subheader("Driver Configuration")

selected_drivers = st.sidebar.multiselect(
    "Select drivers",
    options=list(DRIVERS.keys()),
    default=["Ravi", "Meena", "Control"]
)

st.sidebar.subheader("D3 Configuration")

enable_bad_messages = st.sidebar.checkbox(
    "Enable invalid messages",
    value=False
)

bad_every = 10 if enable_bad_messages else 0

st.sidebar.caption(
    "One message missing the zone field every 10 messages."
    if enable_bad_messages
    else "All messages will be valid."
)

start_button = st.sidebar.button(
    "▶ Start System",
    type="primary",
    use_container_width=True,
    disabled=workers_alive()
)

stop_button = st.sidebar.button(
    "⏹ Stop System",
    use_container_width=True,
    disabled=not workers_alive()
)

if start_button:

    if not selected_drivers:
        st.sidebar.error("Select at least one driver.")

    else:
        start_system(selected_drivers, bad_every)

if stop_button:
    stop_system()


# --------------------------------------------------
# AUTO REFRESH
# --------------------------------------------------

st_autorefresh(
    interval=1000,
    key="dashboard_refresh"
)

collect_logs()


# --------------------------------------------------
# SYSTEM STATUS
# --------------------------------------------------

running = workers_alive()

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "System Status",
        "RUNNING" if running else "STOPPED"
    )

with col2:
    st.metric(
        "Active Drivers",
        sum(
            1 for t in st.session_state.threads[:-1]
            if t.is_alive()
        ) if running else 0
    )

with col3:
    st.metric(
        "Total Events",
        len(st.session_state.logs)
    )


st.divider()


# --------------------------------------------------
# D1 TEST MESSAGE
# --------------------------------------------------

st.subheader("🧪 D1 – Test Topic Routing")

st.write(
    "Send a fixed routing key and verify which drivers receive it."
)

if st.button(
    "Send D1 Test Message: bangalore.koramangala.food",
    disabled=not running
):

    try:

        routing_key = publish_d1_test_message(AMQP_URL)

        st.success(
            f"Published test message: {routing_key}"
        )

    except Exception as e:

        st.error(f"Publishing failed: {e}")


st.caption(
    "Expected recipients when Ravi, Meena and Control are active: "
    "Ravi, Meena and Control."
)


st.divider()


# --------------------------------------------------
# DRIVER DASHBOARD
# --------------------------------------------------

st.subheader("👨‍✈️ Driver Monitoring")

cols = st.columns(3)

for index, (name, pattern) in enumerate(DRIVERS.items()):

    with cols[index % 3]:

        driver_logs = [
            log for log in st.session_state.logs
            if log["component"] == name
        ]

        ack_count = sum(
            1 for log in driver_logs
            if log["message"].startswith("ACK")
        )

        nack_count = sum(
            1 for log in driver_logs
            if log["message"].startswith("NACK")
        )

        is_selected = name in selected_drivers

        with st.container(border=True):

            st.markdown(f"### 👤 {name}")

            st.caption(pattern)

            status = (
                "Connected"
                if running and is_selected
                else "Not active"
            )

            st.write(f"Status: {status}")

            a, b = st.columns(2)

            a.metric("ACK", ack_count)
            b.metric("NACK", nack_count)

            recent = [
                log["message"]
                for log in driver_logs[-5:]
            ]

            with st.expander("View recent messages"):

                if recent:
                    for message in reversed(recent):
                        st.text(message)
                else:
                    st.caption("Waiting for messages...")


st.divider()


# --------------------------------------------------
# LIVE EVENT LOG
# --------------------------------------------------

st.subheader("📡 Live Event Monitor")

tab1, tab2, tab3 = st.tabs([
    "All Events",
    "Dispatcher",
    "ACK / NACK"
])


with tab1:

    for log in reversed(st.session_state.logs[-40:]):

        st.text(
            f"[{log['time']}] "
            f"[{log['component']}] "
            f"{log['message']}"
        )


with tab2:

    dispatcher_logs = [
        log for log in st.session_state.logs
        if log["component"] == "Dispatcher"
    ]

    for log in reversed(dispatcher_logs[-40:]):

        st.text(
            f"[{log['time']}] {log['message']}"
        )


with tab3:

    ack_nack_logs = [
        log for log in st.session_state.logs
        if (
            log["message"].startswith("ACK")
            or log["message"].startswith("NACK")
        )
    ]

    for log in reversed(ack_nack_logs[-40:]):

        st.text(
            f"[{log['time']}] "
            f"[{log['component']}] "
            f"{log['message']}"
        )


# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.divider()

st.caption(
    "RabbitMQ | Pika | Python | Streamlit | "
    "Data Engineering Assignment - Part D"
)
