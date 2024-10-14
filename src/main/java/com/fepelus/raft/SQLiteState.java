package com.fepelus.raft;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class SQLiteState implements PersistedState {

    Connection conn;
    Config config;

    SQLiteState(Config config) {
        this.config = config;
        try {
            conn = DriverManager.getConnection(
                "jdbc:sqlite:" + config.dbFilename()
            );
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(
                    "CREATE TABLE IF NOT EXISTS term (currentTerm INTEGER)"
                );
                var rs = stmt.executeQuery("SELECT count(currentTerm) FROM term");
                rs.next();
                if (rs.getInt(1) == 0) {
                    stmt.execute("INSERT INTO term(currentTerm) VALUES (0)");
                }
                stmt.execute(
                    "CREATE TABLE IF NOT EXISTS log (" +
                    " id INTEGER PRIMARY KEY," +
                    " message TEXT," +
                    " term INTEGER)"
                );
            }
        } catch (SQLException e) {
            throw new RaftException("Could not access DB", e);
        }
    }

    void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RaftException("Could not close DB", e);
        }
    }

    @Override
    public long currentTerm() {
        var sql = "SELECT currentTerm FROM term";
        try (
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(sql)
        ) {
            while (rs.next()) {
                return rs.getLong("currentTerm");
            }
        } catch (SQLException e) {
            System.err.printf("currentTerm %s%n", e.getMessage());
        }
        return 0L;
    }

    @Override
    public void incrementTerm() {
        long currentTerm = currentTerm();

        setTerm(currentTerm + 1);
        Debug.log(config.thisNode(), String.format("CurrentTerm is incremented to %d", currentTerm()));
    }

    @Override
    public void setTerm(long newTerm) {
        String sql = "UPDATE term SET currentTerm = ?";
        try (var pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, newTerm);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RaftException("Could not setTerm", e);
        }
    }

    @Override
    public Log log() {
        return new SQLiteLog(conn);
    }

    @Override
    public String toString() {
        return String.format("{term: %d, log: %s}", currentTerm(), log());
    }
}

class SQLiteLog implements Log {

    Connection conn;

    SQLiteLog(Connection conn) {
        this.conn = conn;
    }

    @Override
    public int size() {
        var sql = "SELECT count(id) FROM log";
        try (
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(sql)
        ) {
            while (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new RaftException("Could not select count from DB", e);
        }
        return 0;
    }

    @Override
    public void add(LogEntry entry) {
        String sql = "INSERT INTO log(message,term) VALUES(?,?)";
        try (var pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, entry.message());
            pstmt.setLong(2, entry.term());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            throw new RaftException("Could not insert into DB", e);
        }
    }

    @Override
    public List<LogEntry> suffixFrom(int index) {
        var sql = "SELECT message, term FROM log WHERE id>?";
        List<LogEntry> output = new ArrayList<>();
        try (var stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, index);
            var rs = stmt.executeQuery();
            while (rs.next()) {
                output.add(
                    new LogEntry(rs.getString("message"), rs.getLong("term"))
                );
            }
        } catch (SQLException e) {
            System.err.printf("suffixFrom %s%n", e.getMessage());
            throw new RaftException("Could not get suffix from ", e);
        }
        return output;
    }

    @Override
    public LogEntry get(int index) {
        var sql = "SELECT message, term FROM log WHERE id=?";
        try (var stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, index + 1);
            var rs = stmt.executeQuery();
            while (rs.next()) {
                return new LogEntry(
                    rs.getString("message"),
                    rs.getLong("term")
                );
            }
        } catch (SQLException e) {
            System.err.printf("Selecting log: %s%n", e.getMessage());
            throw new RaftException("Could not get message", e);
        }
        return null;
    }

    @Override
    public void truncateToLength(int prefixLen) {
        var sql = "DELETE FROM log WHERE id>?";
        try (var stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, prefixLen);
            stmt.execute();
        } catch (SQLException e) {
            System.err.printf("truncate: %s%n", e.getMessage());
            throw new RaftException("Could not truncate", e);
        }
    }

    @Override
    public String toString() {
        return "[" + suffixFrom(0)
                .stream()
                .map(LogEntry::toString)
                .collect(Collectors.joining(", ")) + "]";
    }
}
