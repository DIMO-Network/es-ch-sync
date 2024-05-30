package deviceapi

import (
	"context"
	"fmt"
	"time"

	pb "github.com/DIMO-Network/devices-api/pkg/grpc"
	gocache "github.com/patrickmn/go-cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	deviceTokenCacheKey = "deviceSubject_%s"
	devciceSubCacheKey  = "deviceToken_%d"
	cacheDefaultExp     = 24 * time.Hour
)

// NotFoundError is an error type for when a device's token is not found.
type NotFoundError struct {
	DeviceID string
	TokenId  uint32
}

// Error returns the error message for a NotFoundError.
func (e NotFoundError) Error() string {
	return fmt.Sprintf("device not found id: '%s' tokenID: '%d'", e.DeviceID, e.TokenId)
}

// Is checks if the target error is a NotFoundError.
func (e NotFoundError) Is(target error) bool {
	_, ok := target.(NotFoundError)
	return ok
}

type Service struct {
	memoryCache  *gocache.Cache
	deviceClient pb.UserDeviceServiceClient
}

// NewService API wrapper to call device-telemetry-api to get the userDevices associated with a userId over grpc
func NewService(devicesConn *grpc.ClientConn) *Service {
	c := gocache.New(cacheDefaultExp, 15*time.Minute)

	deviceClient := pb.NewUserDeviceServiceClient(devicesConn)
	return &Service{deviceClient: deviceClient, memoryCache: c}
}

// TokenIDFromSubject gets the tokenID from the subject for a userDevice
func (s *Service) TokenIDFromSubject(ctx context.Context, id string) (uint32, error) {
	var tokenID *uint64
	get, found := s.memoryCache.Get(fmt.Sprintf(deviceTokenCacheKey, id))
	if found {
		tokenID = get.(*uint64)
	} else {
		userDevice, err := s.deviceClient.GetUserDevice(ctx, &pb.GetUserDeviceRequest{
			Id: id,
		})
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return 0, fmt.Errorf("failed to get userDevice: %w", err)
			}
			// store missing tokenID so we don't keep querying
			tokenID = nil
		} else {
			tokenID = userDevice.TokenId
		}
		s.memoryCache.Set(fmt.Sprintf(deviceTokenCacheKey, id), tokenID, 0)
	}

	if tokenID == nil {
		return 0, fmt.Errorf("%w: no tokenID set", NotFoundError{DeviceID: id})
	}
	return uint32(*tokenID), nil
}

// SubjectFromTokenID gets the subject from the tokenID for a vehicle NFT
func (s *Service) SubjectFromTokenID(ctx context.Context, tokenID uint32) (string, error) {
	var deviceID string

	get, found := s.memoryCache.Get(fmt.Sprintf(devciceSubCacheKey, tokenID))
	if found {
		deviceID = get.(string)
	} else {
		userDevice, err := s.deviceClient.GetUserDeviceByTokenId(ctx, &pb.GetUserDeviceByTokenIdRequest{
			TokenId: int64(tokenID),
		})
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return "", fmt.Errorf("failed to get userDevice: %w", err)
			}
			// store missing subject so we don't keep querying
			deviceID = ""
		} else {
			deviceID = userDevice.Id
		}
		s.memoryCache.Set(fmt.Sprintf(devciceSubCacheKey, tokenID), deviceID, 0)
	}
	if deviceID == "" {
		return "", fmt.Errorf("%w: no id set", NotFoundError{TokenId: tokenID})
	}
	return deviceID, nil
}

// Prime caches the tokenID and subject for a userDevice.
func (s *Service) PrimeTokenIDCache(ctx context.Context, tokenID uint32, id string) {
	uintID := uint64(tokenID)
	s.memoryCache.Set(fmt.Sprintf(deviceTokenCacheKey, id), &uintID, -1)
	s.memoryCache.Set(fmt.Sprintf(devciceSubCacheKey, tokenID), id, -1)
}
