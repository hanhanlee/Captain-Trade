import hmac

import streamlit as st


_AUTH_KEY = "authenticated"
_USER_KEY = "username"
_LEGACY_AUTH_KEY = "auth_logged_in"
_LEGACY_USER_KEY = "auth_user"


def _is_local() -> bool:
    try:
        host = st.context.headers.get("host", "")
        return host.startswith("127.0.0.1") or host.startswith("localhost")
    except Exception:
        return True


def _auth_users() -> dict[str, str]:
    try:
        users = st.secrets.get("auth", {})
    except Exception:
        return {}
    return {str(user): str(password) for user, password in dict(users).items()}


def current_user() -> str | None:
    if not st.session_state.get(_AUTH_KEY):
        return None
    return st.session_state.get(_USER_KEY)


def logout() -> None:
    st.session_state.pop(_AUTH_KEY, None)
    st.session_state.pop(_USER_KEY, None)
    st.session_state.pop(_LEGACY_AUTH_KEY, None)
    st.session_state.pop(_LEGACY_USER_KEY, None)


def require_login() -> None:
    if _is_local():
        st.session_state[_AUTH_KEY] = True
        st.session_state[_USER_KEY] = "local"
        return

    if st.session_state.get(_AUTH_KEY):
        user = current_user()
        with st.sidebar:
            st.caption(f"登入帳號：{user}")
            if st.button("登出", key="auth_logout"):
                logout()
                st.rerun()
        return

    users = _auth_users()
    if not users:
        st.error("尚未設定 Streamlit 登入帳號。請建立 .streamlit/secrets.toml 的 [auth] 區塊。")
        st.code('[auth]\nhanhan = "your-password"\nopal = "your-password"', language="toml")
        st.stop()

    st.title("登入")
    st.caption("請輸入帳號密碼後繼續使用。")

    with st.form("streamlit_login_form"):
        username = st.text_input("帳號")
        password = st.text_input("密碼", type="password")
        submitted = st.form_submit_button("登入", type="primary", use_container_width=True)

    if submitted:
        expected = users.get(username)
        if expected is not None and hmac.compare_digest(password, expected):
            st.session_state[_AUTH_KEY] = True
            st.session_state[_USER_KEY] = username
            st.rerun()
        st.error("帳號或密碼錯誤。")

    st.stop()
