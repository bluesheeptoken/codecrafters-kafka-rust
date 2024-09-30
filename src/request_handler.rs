use crate::server;

use server::model;
use server::requests;
use server::responses;

pub fn process_request(request: &requests::Request) -> responses::Response {
    match request {
        requests::Request::ApiVersions(api_versions_request) => {
            responses::Response::ApiVersions(process_api_versions_request(api_versions_request))
        }
        requests::Request::Fetch(fetch_request) => {
            responses::Response::Fetch(process_fetch_request(fetch_request))
        }
    }
}

fn process_api_versions_request(_request: &requests::ApiVersions) -> responses::ApiVersions {
    responses::ApiVersions {
        api_key_versions: vec![model::ApiKeyVariant::Fetch, model::ApiKeyVariant::Versions],
        throttle_time_in_ms: 0,
    }
}

fn process_fetch_request(request: &requests::Fetch) -> responses::Fetch {
    responses::Fetch {
        throttle_time_in_ms: 0,
        session_id: request.session_id,
        topics: request
            .topics
            .iter()
            .map(|topic| responses::fetch::FetchTopicResponse { topic_id: topic.id })
            .collect(),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use super::model::ApiKey;
    use super::requests::RequestHeader;

    #[test]
    fn test_process_request_api_versions() {
        let request = &requests::Request::ApiVersions(requests::ApiVersions {
            header: RequestHeader {
                request_api_key: ApiKey::Versions,
                request_api_version: 4,
                correlation_id: 311908132,
            },
        });

        let expected_response = responses::Response::ApiVersions(responses::ApiVersions {
            api_key_versions: vec![model::ApiKeyVariant::Fetch, model::ApiKeyVariant::Versions],
            throttle_time_in_ms: 0,
        });

        assert_eq!(process_request(request), expected_response);
    }

    #[test]
    fn test_process_request_fetch() {
        let request = &requests::Request::Fetch(requests::Fetch {
            header: RequestHeader {
                request_api_key: ApiKey::Versions,
                request_api_version: 4,
                correlation_id: 311908132,
            },
            session_id: 85,
            topics: vec![model::Topic { id: 37 }],
        });

        let expected_response = responses::Response::Fetch(responses::Fetch {
            throttle_time_in_ms: 0,
            session_id: 85,
            topics: vec![responses::fetch::FetchTopicResponse { topic_id: 37 }],
        });

        assert_eq!(process_request(request), expected_response);
    }
}
