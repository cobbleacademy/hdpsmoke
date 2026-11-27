package com.hsm.edekmigrate;

/**
 * Which rows a command works on: all of them, or one source KEK ({@code --kek}, the
 * name in the source, NULL kek_name counted as the legacy default) and/or one owner
 * app_id ({@code --app}).
 */
public record Scope(String kek, String app) {

    public static final Scope ALL = new Scope(null, null);

    public boolean isAll() {
        return kek == null && app == null;
    }

    @Override
    public String toString() {
        return isAll() ? "all" : (kek == null ? "" : "kek=" + kek) + (kek != null && app != null ? " " : "")
                + (app == null ? "" : "app=" + app);
    }
}
