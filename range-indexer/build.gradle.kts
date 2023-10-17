import com.google.protobuf.gradle.id

plugins {
    java
    idea
    id("io.freefair.lombok") version "8.3"
    id("com.google.cloud.tools.jib") version "3.4.0"
    id("com.google.protobuf") version "0.9.4"
}

group = "com.bakdata.uni"

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

val grpcVersion = "1.58.0"
dependencies {
    implementation(group = "com.bakdata.kafka", name = "streams-bootstrap", version = "2.13.0")
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = "2.20.0")

    val confluentVersion = "7.4.1"
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    implementation(group = "info.picocli", name = "picocli", version = "4.7.1")

    val log4jVersion = "2.20.0"
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)

    implementation(group = "io.grpc", name = "grpc-netty-shaded", version = grpcVersion)
    implementation(group = "io.grpc", name = "grpc-services", version = grpcVersion)
    implementation(group = "io.grpc", name = "grpc-stub", version = grpcVersion)
    compileOnly(group = "org.apache.tomcat", name = "annotations-api", version = "6.0.53")

    implementation(project(":common"))
    implementation(project(":range-lib"))

    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.24.2")

    val kafkaVersion = "3.4.0"
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }

    testImplementation(group = "io.grpc", name = "grpc-testing", version = grpcVersion)
}

tasks {
    compileJava {
        options.encoding = "UTF-8"
    }
    compileTestJava {
        options.encoding = "UTF-8"
    }
    test {
        useJUnitPlatform()
    }
}

jib {
    to {
        image = "raminqaf/range-indexer:1.0.0"
    }
}

protobuf {
    val protobufVersion = "3.24.0"

    protoc {
        artifact = "com.google.protobuf:protoc:${protobufVersion}"
    }
    delete {
        generatedFilesBaseDir
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${grpcVersion}"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc")
            }
            it.outputBaseDir
        }
    }
}

