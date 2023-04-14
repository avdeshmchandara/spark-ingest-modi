Feature: Connection Testing
    Scenario: Postgres Testing
        Given Pipeline ID "80666eff-7048-31ff-84f0-5b3a04bb6421"
        Then Test Postgres Reader
        Then Test Postgres Writer
    Scenario: Sybase Testing
        Given Pipeline ID "3877b605-195c-3918-a465-35f89927acf0"
        Then Test Sybase Reader
        Then Test Sybase Writer
    Scenario: Oracle Testing
        Given Pipeline ID "3877b605-195c-3918-a465-35f89927acf0"
        Then Test Oracle Reader
        Then Test Oracle Writer 