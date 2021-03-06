/* tslint:disable */
/* eslint-disable */
/**
 * Stacks 2.0 Blockchain API
 * This is the documentation for the Stacks 2.0 Blockchain API.  It is comprised of two parts; the Stacks Blockchain API and the Stacks Core API.  [![Run in Postman](https://run.pstmn.io/button.svg)](https://app.getpostman.com/run-collection/614feab5c108d292bffa#?env%5BStacks%20Blockchain%20API%5D=W3sia2V5Ijoic3R4X2FkZHJlc3MiLCJ2YWx1ZSI6IlNUMlRKUkhESE1ZQlE0MTdIRkIwQkRYNDMwVFFBNVBYUlg2NDk1RzFWIiwiZW5hYmxlZCI6dHJ1ZX0seyJrZXkiOiJibG9ja19pZCIsInZhbHVlIjoiMHgiLCJlbmFibGVkIjp0cnVlfSx7ImtleSI6Im9mZnNldCIsInZhbHVlIjoiMCIsImVuYWJsZWQiOnRydWV9LHsia2V5IjoibGltaXRfdHgiLCJ2YWx1ZSI6IjIwMCIsImVuYWJsZWQiOnRydWV9LHsia2V5IjoibGltaXRfYmxvY2siLCJ2YWx1ZSI6IjMwIiwiZW5hYmxlZCI6dHJ1ZX0seyJrZXkiOiJ0eF9pZCIsInZhbHVlIjoiMHg1NDA5MGMxNmE3MDJiNzUzYjQzMTE0ZTg4NGJjMTlhODBhNzk2MzhmZDQ0OWE0MGY4MDY4Y2RmMDAzY2RlNmUwIiwiZW5hYmxlZCI6dHJ1ZX0seyJrZXkiOiJjb250cmFjdF9pZCIsInZhbHVlIjoiU1RKVFhFSlBKUFBWRE5BOUIwNTJOU1JSQkdRQ0ZOS1ZTMTc4VkdIMS5oZWxsb193b3JsZFxuIiwiZW5hYmxlZCI6dHJ1ZX0seyJrZXkiOiJidGNfYWRkcmVzcyIsInZhbHVlIjoiYWJjIiwiZW5hYmxlZCI6dHJ1ZX0seyJrZXkiOiJjb250cmFjdF9hZGRyZXNzIiwidmFsdWUiOiJTVEpUWEVKUEpQUFZETkE5QjA1Mk5TUlJCR1FDRk5LVlMxNzhWR0gxIiwiZW5hYmxlZCI6dHJ1ZX0seyJrZXkiOiJjb250cmFjdF9uYW1lIiwidmFsdWUiOiJoZWxsb193b3JsZCIsImVuYWJsZWQiOnRydWV9LHsia2V5IjoiY29udHJhY3RfbWFwIiwidmFsdWUiOiJzdG9yZSIsImVuYWJsZWQiOnRydWV9LHsia2V5IjoiY29udHJhY3RfbWV0aG9kIiwidmFsdWUiOiJnZXQtdmFsdWUiLCJlbmFibGVkIjp0cnVlfV0=) 
 *
 * The version of the OpenAPI document: 1.0.0
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */

import { exists, mapValues } from '../runtime';
import {
    RosettaAccount,
    RosettaAccountFromJSON,
    RosettaAccountFromJSONTyped,
    RosettaAccountToJSON,
    RosettaOptions,
    RosettaOptionsFromJSON,
    RosettaOptionsFromJSONTyped,
    RosettaOptionsToJSON,
} from './';

/**
 * RosettaConstructionPreprocessResponse contains options that will be sent unmodified to /construction/metadata. If it is not necessary to make a request to /construction/metadata, options should be omitted. Some blockchains require the PublicKey of particular AccountIdentifiers to construct a valid transaction. To fetch these PublicKeys, populate required_public_keys with the AccountIdentifiers associated with the desired PublicKeys. If it is not necessary to retrieve any PublicKeys for construction, required_public_keys should be omitted.
 * @export
 * @interface RosettaConstructionPreprocessResponse
 */
export interface RosettaConstructionPreprocessResponse {
    /**
     * 
     * @type {RosettaOptions}
     * @memberof RosettaConstructionPreprocessResponse
     */
    options?: RosettaOptions;
    /**
     * 
     * @type {Array<RosettaAccount>}
     * @memberof RosettaConstructionPreprocessResponse
     */
    required_public_keys?: Array<RosettaAccount>;
}

export function RosettaConstructionPreprocessResponseFromJSON(json: any): RosettaConstructionPreprocessResponse {
    return RosettaConstructionPreprocessResponseFromJSONTyped(json, false);
}

export function RosettaConstructionPreprocessResponseFromJSONTyped(json: any, ignoreDiscriminator: boolean): RosettaConstructionPreprocessResponse {
    if ((json === undefined) || (json === null)) {
        return json;
    }
    return {
        
        'options': !exists(json, 'options') ? undefined : RosettaOptionsFromJSON(json['options']),
        'required_public_keys': !exists(json, 'required_public_keys') ? undefined : ((json['required_public_keys'] as Array<any>).map(RosettaAccountFromJSON)),
    };
}

export function RosettaConstructionPreprocessResponseToJSON(value?: RosettaConstructionPreprocessResponse | null): any {
    if (value === undefined) {
        return undefined;
    }
    if (value === null) {
        return null;
    }
    return {
        
        'options': RosettaOptionsToJSON(value.options),
        'required_public_keys': value.required_public_keys === undefined ? undefined : ((value.required_public_keys as Array<any>).map(RosettaAccountToJSON)),
    };
}


