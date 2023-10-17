plugins {
    `java-library`
    idea
    id("io.freefair.lombok") version "8.3"
}

group = "com.bakdata.uni"

repositories {
    mavenCentral()
}

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}


dependencies {
    // kafka
    val kafkaVersion = "3.4.0"
    implementation(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)

    // avro
    implementation("org.apache.avro:avro:1.11.3")

    // common
    implementation("com.google.guava:guava:32.1.2-jre")

    // testing
    val junitVersion = "5.9.1"
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "org.junit-pioneer", name = "junit-pioneer", version = "1.9.1")
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.23.1")
    val mockitoVersion = "4.11.0"
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
}

tasks.test {
    useJUnitPlatform()
}
