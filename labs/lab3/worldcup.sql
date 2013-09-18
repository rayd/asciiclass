DROP TABLE IF EXISTS worldcup;

CREATE TABLE worldcup (
    place int,
    year int,
    team text
);

CREATE INDEX team_idx ON worldcup (team);
