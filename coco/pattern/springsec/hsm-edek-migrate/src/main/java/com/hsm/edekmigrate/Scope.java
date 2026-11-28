package com.hsm.edekmigrate;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Which rows a command works on. Each set limits the rows when it isn't empty, and the
 * limits combine (a row must match all of them):
 *
 * <ul>
 *   <li>{@code keks}: source KEK names ({@code --kek}); a NULL kek_name counts as the
 *       legacy default.</li>
 *   <li>{@code apps}: owner app_ids ({@code --app}).</li>
 *   <li>{@code ids}: edek_ids ({@code --ids <file>}), for a trial promote of a sample.</li>
 * </ul>
 */
public record Scope(Set<String> keks, Set<String> apps, Set<UUID> ids) {

    public static final Scope ALL = new Scope(Set.of(), Set.of(), Set.of());

    /** Copies each set, sorted and unmodifiable. */
    public Scope {
        keks = Collections.unmodifiableSortedSet(new TreeSet<>(keks));
        apps = Collections.unmodifiableSortedSet(new TreeSet<>(apps));
        ids = Collections.unmodifiableSortedSet(new TreeSet<>(ids));
    }

    /** One KEK and/or one app; null for no limit. */
    public Scope(String kek, String app) {
        this(kek == null ? Set.of() : Set.of(kek), app == null ? Set.of() : Set.of(app), Set.of());
    }

    public boolean isAll() {
        return keks.isEmpty() && apps.isEmpty() && ids.isEmpty();
    }

    String[] kekArray() {
        return keks.toArray(String[]::new);
    }

    String[] appArray() {
        return apps.toArray(String[]::new);
    }

    /** As text, for {@code = ANY(CAST(? AS uuid[]))}. */
    String[] idArray() {
        return ids.stream().map(UUID::toString).toArray(String[]::new);
    }

    @Override
    public String toString() {
        if (isAll()) {
            return "all";
        }
        StringBuilder sb = new StringBuilder();
        if (!keks.isEmpty()) {
            sb.append("kek=").append(String.join(",", keks));
        }
        if (!apps.isEmpty()) {
            sb.append(sb.isEmpty() ? "" : " ").append("app=").append(String.join(",", apps));
        }
        if (!ids.isEmpty()) {
            sb.append(sb.isEmpty() ? "" : " ").append("ids=").append(ids.size());
        }
        return sb.toString();
    }
}
