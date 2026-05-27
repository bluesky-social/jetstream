package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApp_Help(t *testing.T) {
	t.Parallel()
	cmd := newApp()
	require.NoError(t, cmd.Run(context.Background(), []string{"simulator", "--help"}))
}
