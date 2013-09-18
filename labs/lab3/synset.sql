DROP TABLE IF EXISTS synset;

CREATE TABLE synset (
    word text,
    def text
);

CREATE INDEX word_idx ON synset (word);
