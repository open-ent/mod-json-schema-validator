/*
 * Copyright © WebServices pour l'Éducation, 2018
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.opendigitaleducation.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.vertx.java.busmods.BusModBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static fr.wseduc.webutils.Utils.getOrElse;
import static fr.wseduc.webutils.Utils.isEmpty;


public class JsonSchemaValidator extends BusModBase implements Handler<Message<JsonObject>> {

	private Map<String, JsonSchema> schemas;
	private JsonSchemaFactory schemaFactory;

	@Override
	public void start() {
		super.start();
		this.schemas = new HashMap<>();
		this.schemaFactory = JsonSchemaFactory.byDefault();
		vertx.eventBus().consumer(config.getString("address", "json.schema.validator"), this);
	}

	@Override
	public void handle(Message<JsonObject> message) {
		String action = message.body().getString("action", "");
		switch (action) {
			case "validate" :
				validate(message);
				break;
			case "getSchemaKeys" :
				getSchemaKeys(message);
				break;
			case "addSchema" :
				addSchema(message);
				break;
			default :
				sendError(message, "invalid.action");
		}
	}

	private void validate(Message<JsonObject> message) {
		final String key = message.body().getString("key");
		if (isEmpty(key)) {
			sendError(message, "invalid.key");
			return;
		}
		final JsonSchema schema = schemas.get(key);
		if (schema == null) {
			sendError(message, "invalid.schema.key");
			return;
		}
		final Object json = message.body().getValue("json");
		if (json == null) {
			sendError(message, "missing.json");
			return;
		}
		try {
			final ProcessingReport report = schema.validate(JsonLoader.fromString(Json.encode(json)));
			if (report.isSuccess()) {
				sendOK(message);
			} else {
				sendError(message, report.toString());
			}
		} catch (Exception e) {
			sendError(message, "validation.error", e);
		}
	}

	private void getSchemaKeys(Message<JsonObject> message) {
		sendOK(message, new JsonObject().put("schemas", new JsonArray(new ArrayList<>(schemas.keySet()))));
	}

	private void addSchema(Message<JsonObject> message) {
		final String key = message.body().getString("key");
		if (isEmpty(key)) {
			sendError(message, "invalid.key");
			return;
		}
		if (schemas.containsKey(key) && !getOrElse(message.body().getBoolean("overwrite"), false)) {
			sendError(message, "key.already.exists");
			return;
		}
		final JsonObject schema = message.body().getJsonObject("jsonSchema");
		if (schema == null || schema.size() == 0) {
			sendError(message, "invalid.schema");
			return;
		}
		try {
			final JsonNode jsonNode = JsonLoader.fromString(schema.encode());
			if (!schemaFactory.getSyntaxValidator().schemaIsValid(jsonNode)) {
				sendError(message, "invalid.schema.syntax");
			} else {
				schemas.put(key, schemaFactory.getJsonSchema(jsonNode));
				sendOK(message);
			}
		} catch (Exception e) {
			sendError(message, "schema.error", e);
		}
	}

}
