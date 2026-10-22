package core

// StageScopeKey is interpreted only in an authenticated Plan.PublishContexts.
const StageScopeKey = "ssf.dev/stage-scope"
const StageScopeBinding = "binding-v1"

// PublishedFilesystemUUIDKey binds a driver-created filesystem to preparation.
// It is returned by authenticated ControllerPublish and included in the plan.
const PublishedFilesystemUUIDKey = "ssf.dev/published-filesystem-uuid"
