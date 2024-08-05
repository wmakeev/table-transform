export class AsyncChannelClosedError extends Error {
  constructor(public channelName?: string) {
    super(
      typeof channelName === 'string' && channelName !== ''
        ? `Channel("${channelName}") is closed`
        : 'Channel is closed'
    )
  }
}
