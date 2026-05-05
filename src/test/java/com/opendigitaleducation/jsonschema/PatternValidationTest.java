package com.opendigitaleducation.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Vérifie que la validation de pattern (regexp) dans les schémas JSON
 * fonctionne correctement sous JDK 21, où Rhino/Nashorn n'est plus disponible.
 *
 * Schéma testé : init_structure.json (vie-scolaire)
 * Pattern : "^([0-1][0-9]|2[0-3]):[0-5][0-9]$" (format HH:MM en 24h)
 */
public class PatternValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static JsonSchema schema;

    @BeforeClass
    public static void loadSchema() throws Exception {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        try (InputStream is = PatternValidationTest.class.getResourceAsStream("/jsonschema/init_structure.json")) {
            JsonNode schemaNode = MAPPER.readTree(is);
            schema = factory.getSchema(schemaNode);
        }
    }

    // ---- cas valides -------------------------------------------------------

    @Test
    public void validPayload_shouldPassValidation() throws Exception {
        String json = buildPayload("08:30", "12:00", "13:30", "17:00");
        Set<ValidationMessage> errors = schema.validate(MAPPER.readTree(json));
        assertTrue("Un JSON valide ne doit produire aucune erreur, obtenu : " + errors, errors.isEmpty());
    }

    @Test
    public void boundaryHours_00and23_shouldPassValidation() throws Exception {
        String json = buildPayload("00:00", "12:00", "13:00", "23:59");
        Set<ValidationMessage> errors = schema.validate(MAPPER.readTree(json));
        assertTrue("Les bornes 00:00 et 23:59 doivent être acceptées, obtenu : " + errors, errors.isEmpty());
    }

    // ---- cas invalides : pattern regexp ------------------------------------

    @Test
    public void missingLeadingZero_shouldFailPatternValidation() throws Exception {
        // "8:30" au lieu de "08:30" — régression JDK 21 : le pattern n'était pas évalué
        String json = buildPayload("8:30", "12:00", "13:30", "17:00");
        Set<ValidationMessage> errors = schema.validate(MAPPER.readTree(json));
        assertFalse("'8:30' doit échouer la validation du pattern (zéro manquant)", errors.isEmpty());
    }

    @Test
    public void hourOver23_shouldFailPatternValidation() throws Exception {
        String json = buildPayload("25:00", "12:00", "13:30", "17:00");
        Set<ValidationMessage> errors = schema.validate(MAPPER.readTree(json));
        assertFalse("'25:00' doit échouer la validation du pattern (heure > 23)", errors.isEmpty());
    }

    @Test
    public void minuteOver59_shouldFailPatternValidation() throws Exception {
        String json = buildPayload("08:60", "12:00", "13:30", "17:00");
        Set<ValidationMessage> errors = schema.validate(MAPPER.readTree(json));
        assertFalse("'08:60' doit échouer la validation du pattern (minute > 59)", errors.isEmpty());
    }

    @Test
    public void freeTextInsteadOfTime_shouldFailPatternValidation() throws Exception {
        String json = buildPayload("matin", "12:00", "13:30", "17:00");
        Set<ValidationMessage> errors = schema.validate(MAPPER.readTree(json));
        assertFalse("Une chaîne libre doit échouer la validation du pattern", errors.isEmpty());
    }

    // ---- cas invalides : autres contraintes --------------------------------

    @Test
    public void invalidEnumValue_shouldFailValidation() throws Exception {
        String json = "{"
                + "\"schoolYear\":{\"startDate\":\"2024-09-01\",\"endDate\":\"2025-07-04\"},"
                + "\"timetable\":{"
                +   "\"morning\":{\"startHour\":\"08:30\",\"endHour\":\"12:00\"},"
                +   "\"afternoon\":{\"startHour\":\"13:30\",\"endHour\":\"17:00\"},"
                +   "\"fullDays\":[\"LUNDI\"],"
                +   "\"halfDays\":[]"
                + "},"
                + "\"holidays\":{\"system\":\"FRENCH\",\"zone\":\"Zone A\"},"
                + "\"initType\":\"ONE_D\""
                + "}";
        Set<ValidationMessage> errors = schema.validate(MAPPER.readTree(json));
        assertFalse("'LUNDI' n'est pas une valeur autorisée dans l'enum fullDays", errors.isEmpty());
    }

    @Test
    public void missingRequiredField_shouldFailValidation() throws Exception {
        String json = "{"
                + "\"timetable\":{"
                +   "\"morning\":{\"startHour\":\"08:30\",\"endHour\":\"12:00\"},"
                +   "\"afternoon\":{\"startHour\":\"13:30\",\"endHour\":\"17:00\"},"
                +   "\"fullDays\":[\"MONDAY\"],"
                +   "\"halfDays\":[]"
                + "},"
                + "\"holidays\":{\"system\":\"FRENCH\",\"zone\":\"Zone A\"},"
                + "\"initType\":\"ONE_D\""
                + "}";
        Set<ValidationMessage> errors = schema.validate(MAPPER.readTree(json));
        assertFalse("Le champ 'schoolYear' est requis", errors.isEmpty());
    }

    // ---- helper ------------------------------------------------------------

    private static String buildPayload(String morningStart, String morningEnd,
                                       String afternoonStart, String afternoonEnd) {
        return "{"
                + "\"schoolYear\":{\"startDate\":\"2024-09-01\",\"endDate\":\"2025-07-04\"},"
                + "\"timetable\":{"
                +   "\"morning\":{\"startHour\":\"" + morningStart + "\",\"endHour\":\"" + morningEnd + "\"},"
                +   "\"afternoon\":{\"startHour\":\"" + afternoonStart + "\",\"endHour\":\"" + afternoonEnd + "\"},"
                +   "\"fullDays\":[\"MONDAY\",\"TUESDAY\",\"THURSDAY\",\"FRIDAY\"],"
                +   "\"halfDays\":[\"WEDNESDAY\"]"
                + "},"
                + "\"holidays\":{\"system\":\"FRENCH\",\"zone\":\"Zone A\"},"
                + "\"initType\":\"ONE_D\""
                + "}";
    }
}
