
import { Boom } from '@hapi/boom'
import NodeCache from 'node-cache'
import { proto } from '../../WAProto'
import { DEFAULT_CACHE_TTLS, WA_DEFAULT_EPHEMERAL } from '../Defaults'
import { AnyMessageContent, Media, MediaConnInfo, MessageReceiptType, MessageRelayOptions, MiscMessageGenerationOptions, SocketConfig, WAMediaUploadFunctionOpts, WAMessageKey } from '../Types'
import { aggregateMessageKeysNotFromMe, assertMediaContent, bindWaitForEvent, decryptMediaRetryData, delay, encodeSignedDeviceIdentity, encodeWAMessage, encryptMediaRetryRequest, extractDeviceJids, generateMessageID, generateWAMessage, generateWAMessageFromContent, getContentType, getStatusCodeForMediaRetry, getUrlFromDirectPath, getWAUploadToServer, parseAndInjectE2ESessions, unixTimestampSeconds, normalizeMessageContent } from '../Utils'
import { getUrlInfo } from '../Utils/link-preview'
import { areJidsSameUser, BinaryNode, BinaryNodeAttributes, getBinaryNodeChild, getBinaryNodeChildren, isJidGroup, isJidNewsLetter, isJidUser, jidDecode, jidEncode, jidNormalizedUser, JidWithDevice, S_WHATSAPP_NET, STORIES_JID } from '../WABinary'
import { makeNewsletterSocket } from './newsletter'
import ListType = proto.Message.ListMessage.ListType;
import { Readable } from 'stream'

export const makeMessagesSocket = (config: SocketConfig) => {
	const {
		logger,
		linkPreviewImageThumbnailWidth,
		generateHighQualityLinkPreview,
		options: axiosOptions,
		patchMessageBeforeSending,
	} = config
	const sock = makeNewsletterSocket(config)
	const {
		ev,
		authState,
		processingMutex,
		signalRepository,
		upsertMessage,
		query,
		fetchPrivacySettings,
		generateMessageTag,
		sendNode,
		groupMetadata,
		groupToggleEphemeral
	} = sock


	const userDevicesCache = config.userDevicesCache || new NodeCache({
		stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES, // 5 minutes
		useClones: false
	})

	let mediaConn: Promise<MediaConnInfo>
	const refreshMediaConn = async(forceGet = false) => {
		const media = await mediaConn
		if(!media || forceGet || (new Date().getTime() - media.fetchDate.getTime()) > media.ttl * 1000) {
			mediaConn = (async() => {
				const result = await query({
					tag: 'iq',
					attrs: {
						type: 'set',
						xmlns: 'w:m',
						to: S_WHATSAPP_NET,
					},
					content: [ { tag: 'media_conn', attrs: { } } ]
				})
				const mediaConnNode = getBinaryNodeChild(result, 'media_conn')
				const node: MediaConnInfo = {
					hosts: getBinaryNodeChildren(mediaConnNode, 'host').map(
						({ attrs }) => ({
							hostname: attrs.hostname,
							maxContentLengthBytes: +attrs.maxContentLengthBytes,
						})
					),
					auth: mediaConnNode!.attrs.auth,
					ttl: +mediaConnNode!.attrs.ttl,
					fetchDate: new Date()
				}
				logger.debug('fetched media conn')
				return node
			})()
		}

		return mediaConn
	}

	/**
     * generic send receipt function
     * used for receipts of phone call, read, delivery etc.
     * */
	const sendReceipt = async(jid: string, participant: string | undefined, messageIds: string[], type: MessageReceiptType) => {
		const node: BinaryNode = {
			tag: 'receipt',
			attrs: {
				id: messageIds[0],
			},
		}
		const isReadReceipt = type === 'read' || type === 'read-self'
		if(isReadReceipt) {
			node.attrs.t = unixTimestampSeconds().toString()
		}

		if(type === 'sender' && isJidUser(jid)) {
			node.attrs.recipient = jid
			node.attrs.to = participant!
		} else {
			node.attrs.to = jid
			if(participant) {
				node.attrs.participant = participant
			}
		}

		if(type) {
			node.attrs.type = isJidNewsLetter(jid) ? 'read-self' : type
		}

		const remainingMessageIds = messageIds.slice(1)
		if(remainingMessageIds.length) {
			node.content = [
				{
					tag: 'list',
					attrs: { },
					content: remainingMessageIds.map(id => ({
						tag: 'item',
						attrs: { id }
					}))
				}
			]
		}

		logger.debug({ attrs: node.attrs, messageIds }, 'sending receipt for messages')
		await sendNode(node)
	}

	/** Correctly bulk send receipts to multiple chats, participants */
	const sendReceipts = async(keys: WAMessageKey[], type: MessageReceiptType) => {
		const recps = aggregateMessageKeysNotFromMe(keys)
		for(const { jid, participant, messageIds } of recps) {
			await sendReceipt(jid, participant, messageIds, type)
		}
	}

	/** Bulk read messages. Keys can be from different chats & participants */
	const readMessages = async(keys: WAMessageKey[]) => {
		const privacySettings = await fetchPrivacySettings()
		// based on privacy settings, we have to change the read type
		const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self'
		await sendReceipts(keys, readType)
 	}

	/** Fetch all the devices we've to send a message to */
	const getUSyncDevices = async(jids: string[], useCache: boolean, ignoreZeroDevices: boolean) => {
		const deviceResults: JidWithDevice[] = []

		if(!useCache) {
			logger.debug('not using cache for devices')
		}

		const users: BinaryNode[] = []
		jids = Array.from(new Set(jids))
		for(let jid of jids) {
			const user = jidDecode(jid)?.user
			jid = jidNormalizedUser(jid)

			const devices = userDevicesCache.get<JidWithDevice[]>(user!)
			if(devices && useCache) {
				deviceResults.push(...devices)

				logger.trace({ user }, 'using cache for devices')
			} else {
				users.push({ tag: 'user', attrs: { jid } })
			}
		}

		if(!users.length) {
			return deviceResults
		}

		const iq: BinaryNode = {
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'get',
				xmlns: 'usync',
			},
			content: [
				{
					tag: 'usync',
					attrs: {
						sid: generateMessageTag(),
						mode: 'query',
						last: 'true',
						index: '0',
						context: 'message',
					},
					content: [
						{
							tag: 'query',
							attrs: { },
							content: [
								{
									tag: 'devices',
									attrs: { version: '2' }
								}
							]
						},
						{ tag: 'list', attrs: { }, content: users }
					]
				},
			],
		}
		const result = await query(iq)
		const extracted = extractDeviceJids(result, authState.creds.me!.id, ignoreZeroDevices)
		const deviceMap: { [_: string]: JidWithDevice[] } = {}

		for(const item of extracted) {
			deviceMap[item.user] = deviceMap[item.user] || []
			deviceMap[item.user].push(item)

			deviceResults.push(item)
		}

		for(const key in deviceMap) {
			userDevicesCache.set(key, deviceMap[key])
		}

		return deviceResults
	}


	const assertSessions = async(jids: string[], force: boolean) => {
		let didFetchNewSession = false
		let jidsRequiringFetch: string[] = []
		if(force) {
			jidsRequiringFetch = jids
		} else {
			const addrs = jids.map(jid => (
				signalRepository
					.jidToSignalProtocolAddress(jid)
			))
			const sessions = await authState.keys.get('session', addrs)
			for(const jid of jids) {
				const signalId = signalRepository
					.jidToSignalProtocolAddress(jid)
				if(!sessions[signalId]) {
					jidsRequiringFetch.push(jid)
				}
			}
		}

		if(jidsRequiringFetch.length) {
			logger.debug({ jidsRequiringFetch }, 'fetching sessions')
			const result = await query({
				tag: 'iq',
				attrs: {
					xmlns: 'encrypt',
					type: 'get',
					to: S_WHATSAPP_NET,
				},
				content: [
					{
						tag: 'key',
						attrs: { },
						content: jidsRequiringFetch.map(
							jid => ({
								tag: 'user',
								attrs: { jid },
							})
						)
					}
				]
			})
			await parseAndInjectE2ESessions(result, signalRepository)

			didFetchNewSession = true
		}

		return didFetchNewSession
	}

	const sendPeerDataOperationMessage = async(
		pdoMessage: proto.Message.IPeerDataOperationRequestMessage
	): Promise<string> => {
		//TODO: for later, abstract the logic to send a Peer Message instead of just PDO - useful for App State Key Resync with phone
		if(!authState.creds.me?.id) {
			throw new Boom('Not authenticated')
		}

		const protocolMessage: proto.IMessage = {
			protocolMessage: {
				peerDataOperationRequestMessage: pdoMessage,
				type: proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE
			}
		}

		const meJid = jidNormalizedUser(authState.creds.me.id)

		const msgId = await relayMessage(meJid, protocolMessage, {
			additionalAttributes: {
				category: 'peer',
				// eslint-disable-next-line camelcase
				push_priority: 'high_force',
			},
		})

		return msgId!
	}

	const createParticipantNodes = async(
		jids: string[],
		message: proto.IMessage,
		extraAttrs?: BinaryNode['attrs']
	) => {
		const patched = await patchMessageBeforeSending(message, jids)
		const bytes = encodeWAMessage(patched)

		let shouldIncludeDeviceIdentity = false
		const nodes = await Promise.all(
			jids.map(
				async jid => {
					const { type, ciphertext } = await signalRepository
						.encryptMessage({ jid, data: bytes })
					if(type === 'pkmsg') {
						shouldIncludeDeviceIdentity = true
					}

					const node: BinaryNode = {
						tag: 'to',
						attrs: { jid },
						content: [{
							tag: 'enc',
							attrs: {
								v: '2',
								type,
								...extraAttrs || {}
							},
							content: ciphertext
						}]
					}
					return node
				}
			)
		)
		return { nodes, shouldIncludeDeviceIdentity }
	} //apela

	const relayMessage = async(
		jid: string,
		message: proto.IMessage,
		{ messageId: msgId, participant, additionalAttributes, additionalNodes, useUserDevicesCache, cachedGroupMetadata, statusJidList }: MessageRelayOptions
	) => {
		const meId = authState.creds.me!.id

		let shouldIncludeDeviceIdentity = false

		const { user, server } = jidDecode(jid)!
		const statusJid = 'status@broadcast'
		const isGroup = server === 'g.us'
		const isStatus = jid === statusJid
		const isLid = server === 'lid'
        const isPrivate = server === 's.whatsapp.net'
		const isNewsletter = server === 'newsletter'

		msgId = msgId || generateMessageID()
		useUserDevicesCache = useUserDevicesCache !== false

		const participants: BinaryNode[] = []
		const destinationJid = (!isStatus) ? jidEncode(user, isLid ? 'lid' : isGroup ? 'g.us' : isNewsletter ? 'newsletter' : 's.whatsapp.net') : statusJid
		const binaryNodeContent: BinaryNode[] = []
		const devices: JidWithDevice[] = []		

		const meMsg: proto.IMessage = {
			deviceSentMessage: {
				destinationJid,
				message
			}
		}

		if(participant) {
			// when the retry request is not for a group
			// only send to the specific device that asked for a retry
			// otherwise the message is sent out to every device that should be a recipient
			if(!isGroup && !isStatus) {
				additionalAttributes = { ...additionalAttributes, 'device_fanout': 'false' }
			}

			const { user, device } = jidDecode(participant.jid)!
			devices.push({ user, device })
		}

		await authState.keys.transaction(
			async() => {
				const mediaType = getMediaType(message)
				if(isGroup || isStatus) {
					const [groupData, senderKeyMap] = await Promise.all([
						(async() => {
							let groupData = cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined
							if(groupData) {
								logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata')
							}

							if(!groupData && !isStatus) {
								groupData = await groupMetadata(jid)
							}

							return groupData
						})(),
						(async() => {
							if(!participant && !isStatus) {
								const result = await authState.keys.get('sender-key-memory', [jid])
								return result[jid] || { }
							}

							return { }
						})()
					])

					if(!participant) {
						const participantsList = (groupData && !isStatus) ? groupData.participants.map(p => p.id) : []
						if(isStatus && statusJidList) {
							participantsList.push(...statusJidList)
						}

						const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false)
						devices.push(...additionalDevices)
					}

					const patched = await patchMessageBeforeSending(message, devices.map(d => jidEncode(d.user, isLid ? 'lid' : isGroup ? 'g.us' : isNewsletter ? 'newsletter' : 's.whatsapp.net', d.device)))
					const bytes = encodeWAMessage(patched)

					const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage(
						{
							group: destinationJid,
							data: bytes,
							meId,
						}
					)

					const senderKeyJids: string[] = []
					// ensure a connection is established with every device
					for(const { user, device } of devices) {
						const jid = jidEncode(user, isLid ? 'lid' : 's.whatsapp.net', device)
						if(!senderKeyMap[jid] || !!participant) {
							senderKeyJids.push(jid)
							// store that this person has had the sender keys sent to them
							senderKeyMap[jid] = true
						}
					}

					// if there are some participants with whom the session has not been established
					// if there are, we re-send the senderkey
					if(senderKeyJids.length) {
						logger.debug({ senderKeyJids }, 'sending new sender key')

						const senderKeyMsg: proto.IMessage = {
							senderKeyDistributionMessage: {
								axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
								groupId: destinationJid
							}
						}

						await assertSessions(senderKeyJids, false)

						const result = await createParticipantNodes(senderKeyJids, senderKeyMsg, mediaType ? { mediatype: mediaType } : undefined)
						shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity

						participants.push(...result.nodes)
					}

					binaryNodeContent.push({
						tag: 'enc',
						attrs: { v: '2', type: 'skmsg' },
						content: ciphertext
					})

					await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } })
				} else if (isNewsletter) {
					// Message edit
					if (message.protocolMessage?.editedMessage) {
						msgId = message.protocolMessage.key?.id!
						message = message.protocolMessage.editedMessage
					}

					// Message delete
					if (message.protocolMessage?.type === proto.Message.ProtocolMessage.Type.REVOKE) {
						msgId = message.protocolMessage.key?.id!
						message = {}
					}

					const patched = await patchMessageBeforeSending(message, [])
					const bytes = proto.Message.encode(patched).finish()

					binaryNodeContent.push({
						tag: 'plaintext',
						attrs: mediaType ? { mediatype: mediaType } : {},
						content: bytes
					})
				} else {
					const { user: meUser, device: meDevice } = jidDecode(meId)!

					if(!participant) {
						devices.push({ user })
						// do not send message to self if the device is 0 (mobile)
						if(meDevice !== undefined && meDevice !== 0) {
							devices.push({ user: meUser })
						}

						const additionalDevices = await getUSyncDevices([ meId, jid ], !!useUserDevicesCache, true)
						devices.push(...additionalDevices)
					}

					const allJids: string[] = []
					const meJids: string[] = []
					const otherJids: string[] = []
					for(const { user, device } of devices) {
						const isMe = user === meUser
						const jid = jidEncode(isMe && isLid ? authState.creds?.me?.lid!.split(':')[0] || user : user, isLid ? 'lid' : isGroup ? 'g.us' : isNewsletter ? 'newsletter' : 's.whatsapp.net', device)
						if(isMe) {
							meJids.push(jid)
						} else {
							otherJids.push(jid)
						}

						allJids.push(jid)
					}

					await assertSessions(allJids, false)

					const [
						{ nodes: meNodes, shouldIncludeDeviceIdentity: s1 },
						{ nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }
					] = await Promise.all([
						createParticipantNodes(meJids, meMsg, mediaType ? { mediatype: mediaType } : undefined),
						createParticipantNodes(otherJids, message, mediaType ? { mediatype: mediaType } : undefined)
					])
					participants.push(...meNodes)
					participants.push(...otherNodes)

					shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2
				}

				if(participants.length) {
					binaryNodeContent.push({
						tag: 'participants',
						attrs: { },
						content: participants
					})
				}

				const stanza: BinaryNode = {
					tag: 'message',
					attrs: {
						id: msgId!,
						type: isNewsletter ? getTypeMessage(message) : 'text',
						...(additionalAttributes || {})
					},
					content: binaryNodeContent
				}
				
				// if the participant to send to is explicitly specified (generally retry recp)
				// ensure the message is only sent to that person
				// if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
				if(participant) {
					if(isJidGroup(destinationJid)) {
						stanza.attrs.to = destinationJid
						stanza.attrs.participant = participant.jid
					} else if(areJidsSameUser(participant.jid, meId)) {
						stanza.attrs.to = participant.jid
						stanza.attrs.recipient = destinationJid
					} else {
						stanza.attrs.to = participant.jid
					}
				} else {
					stanza.attrs.to = destinationJid
				}

				if(shouldIncludeDeviceIdentity) {
					(stanza.content as BinaryNode[]).push({
						tag: 'device-identity',
						attrs: { },
						content: encodeSignedDeviceIdentity(authState.creds.account!, true)
					})

					logger.debug({ jid }, 'adding device identity')
				}
			  
				if(additionalNodes && additionalNodes.length > 0) {
                      (stanza.content as BinaryNode[]).push(...additionalNodes);
                }
                const inMsg = normalizeMessageContent(message)!
                const key = getContentType(inMsg)!
                if(!isNewsletter && (key === 'interactiveMessage' || key === 'buttonsMessage')) {
                    const nativeNode = {
						  tag: 'biz',
						  attrs: {},
					      content: [{
							  tag: 'interactive',
						  	  attrs: {
				   				  type: 'native_flow',
			      				  v: '1'
							  },
							  content: [{
			   					  tag: 'native_flow',
			   					  attrs: { 
			   					     name: 'quick_reply',
			   				      }
							  }]
    					  }]
				    }
                    const resultNativeNode = filterNativeNode(additionalNodes);
                    if(resultNativeNode && additionalNodes && additionalNodes.length > 0) {
				        (stanza.content as BinaryNode[]).push(...resultNativeNode);
				    } else {
				        (stanza.content as BinaryNode[]).push(nativeNode);
				    }
				}  
				if(isPrivate) {
				    const botNode = { 
				          tag: 'bot', 
				          attrs: { biz_bot: '1' }
				    };
                    const resultBotNode = filterBotNode(additionalNodes);
                    if(resultBotNode && additionalNodes && additionalNodes.length > 0) {
                      (stanza.content as BinaryNode[]).push(...resultBotNode);
                    } else {
                      (stanza.content as BinaryNode[]).push(botNode)
                    }
				} 
				
                const buttonType = getButtonType(message)
				if(buttonType) {
					(stanza.content as BinaryNode[]).push({
						tag: 'biz',
						attrs: { },
						content: [
							{
								tag: buttonType,
								attrs: getButtonArgs(message),
							}
						]
					})

					logger.debug({ jid }, 'adding business node')
				}

				logger.debug({ msgId }, `sending message to ${participants.length} devices`)

				await sendNode(stanza)
			}
		)

		return msgId
	}


    const filterNativeNode = (nodeContent) => {
          if (Array.isArray(nodeContent)) {
               return nodeContent!.filter((item) => {
                    if (item!.tag === 'biz' && (item!.content && item!.content!.filter((tag) => { tag!.tag === 'interactive' && tag!.attrs!.type === 'native_flow' && tag!.attrs!.v === '1' }))) {
                         return false;
                    }
               return true;
               });
          } else {
               return nodeContent;
          }
    };
    
    const filterBotNode = (nodeContent) => {
          if (Array.isArray(nodeContent)) {
               return nodeContent!.filter((item) => {
                    if (item!.tag === 'bot' && item!.attrs!.biz_bot === '1') {
                         return false;
                    }
               return true;
               });
          } else {
               return nodeContent;
          }
    };
    
	const getTypeMessage = (msg: proto.IMessage) => {
		if (msg.viewOnceMessage) {
			return getTypeMessage(msg.viewOnceMessage.message!)
		} else if (msg.viewOnceMessageV2) {
			return getTypeMessage(msg.viewOnceMessageV2.message!)
		} else if (msg.viewOnceMessageV2Extension) {
			return getTypeMessage(msg.viewOnceMessageV2Extension.message!)
		} else if (msg.ephemeralMessage) {
			return getTypeMessage(msg.ephemeralMessage.message!)
		} else if (msg.documentWithCaptionMessage) {
			return getTypeMessage(msg.documentWithCaptionMessage.message!)
		} else if (msg.reactionMessage) {
			return 'reaction'
		} else if (msg.pollCreationMessage || msg.pollCreationMessageV2 || msg.pollCreationMessageV3 || msg.pollUpdateMessage) {
			return 'poll'
		} else if (getMediaType(msg)) {
			return 'media'
		} else {
			return 'text'
		}
	}

	const getMediaType = (message: proto.IMessage) => {
		if(message.imageMessage) {
			return 'image'
		} else if(message.videoMessage) {
			return message.videoMessage.gifPlayback ? 'gif' : 'video'
		} else if(message.audioMessage) {
			return message.audioMessage.ptt ? 'ptt' : 'audio'
		} else if(message.contactMessage) {
			return 'vcard'
		} else if(message.documentMessage) {
			return 'document'
		} else if(message.contactsArrayMessage) {
			return 'contact_array'
		} else if(message.liveLocationMessage) {
			return 'livelocation'
		} else if(message.stickerMessage) {
			return 'sticker'
		} else if(message.listMessage) {
			return 'list'
		} else if(message.listResponseMessage) {
			return 'list_response'
		} else if(message.buttonsResponseMessage) {
			return 'buttons_response'
		} else if(message.orderMessage) {
			return 'order'
		} else if(message.productMessage) {
			return 'product'
		} else if(message.interactiveResponseMessage) {
			return 'native_flow_response'
		} else if (message.groupInviteMessage) {
            return 'url';
        }
	}

	const getButtonType = (message: proto.IMessage) => {
	    const Msg = normalizeMessageContent(message)!
	    if(Msg.listMessage) {
			return 'list'
		}
	}

	const getButtonArgs = (message: proto.IMessage): BinaryNode['attrs'] => {
		if(message.templateMessage) {
			// TODO: Add attributes
			return {}
		} else if(message.listMessage) {
			const type = message.listMessage.listType
			if(!type) {
				throw new Boom('Expected list type inside message')
			}

			return { v: '2', type: ListType[type].toLowerCase() }
		} else {
			return {}
		}
	}

	const getPrivacyTokens = async(jids: string[]) => {
		const t = unixTimestampSeconds().toString()
		const result = await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				xmlns: 'privacy'
			},
			content: [
				{
					tag: 'tokens',
					attrs: { },
					content: jids.map(
						jid => ({
							tag: 'token',
							attrs: {
								jid: jidNormalizedUser(jid),
								t,
								type: 'trusted_contact'
							}
						})
					)
				}
			]
		})

		return result
	}

	const waUploadToServer = getWAUploadToServer(config, refreshMediaConn)

	const waitForMsgMediaUpdate = bindWaitForEvent(ev, 'messages.media-update')

	return {
		...sock,
		getPrivacyTokens,
		assertSessions,
		relayMessage,
		sendReceipt,
		sendReceipts,
		getButtonArgs,
		readMessages,
		refreshMediaConn,
		getUSyncDevices,
		sendPeerDataOperationMessage,
		createParticipantNodes,
	    waUploadToServer,
		fetchPrivacySettings,
		updateMediaMessage: async(message: proto.IWebMessageInfo) => {
			const content = assertMediaContent(message.message)
			const mediaKey = content.mediaKey!
			const meId = authState.creds.me!.id
			const node = encryptMediaRetryRequest(message.key, mediaKey, meId)

			let error: Error | undefined = undefined
			await Promise.all(
				[
					sendNode(node),
					waitForMsgMediaUpdate(update => {
						const result = update.find(c => c.key.id === message.key.id)
						if(result) {
							if(result.error) {
								error = result.error
							} else {
								try {
									const media = decryptMediaRetryData(result.media!, mediaKey, result.key.id!)
									if(media && media.result !== proto.MediaRetryNotification.ResultType.SUCCESS) {
										const resultStr = proto.MediaRetryNotification.ResultType[media.result]
										throw new Boom(
											`Media re-upload failed by device (${resultStr})`,
											{ data: media, statusCode: getStatusCodeForMediaRetry(media.result) || 404 }
										)
									}

									content.directPath = media.directPath
									content.url = getUrlFromDirectPath(content.directPath!)

									logger.debug({ directPath: media.directPath, key: result.key }, 'media update successful')
								} catch(err) {
									error = err
								}
							}

							return true
						}
					})
				]
			)

			if(error) {
				throw error
			}

			ev.emit('messages.update', [
				{ key: message.key, update: { message: message.message } }
			])

			return message
		},
		sendStatusMentions: async(
		   content: AnyMessageContent, 
		   jids: string[] = []
		) => { 
		   const userJid = jidNormalizedUser(authState.creds.me!.id) 		       
           let allUsers: string[] = [];

           for(const id of jids) {
		      const { user, server } = jidDecode(id)!
		      const isGroup = server === 'g.us'
              const isPrivate = server === 's.whatsapp.net'
              if(isGroup) {
                 let userId = await groupMetadata(id)
                 let participant = await userId.participants
                 let users = await Promise.all(participant.map(u => jidNormalizedUser(u.id))); 
                 allUsers = [...allUsers as string[], ...users as string[]];
              } else if(isPrivate) {
                 let users = await Promise.all(jids.map(id => id.replace(/\b\d{18}@.{4}\b/g, '')));
                 allUsers = [...allUsers as string[], ...users as string[]];
              }
              if(!allUsers.find(user => user.includes(userJid))) {
                 (allUsers as string[]).push(userJid)
              }
           };
           const getRandomHexColor = () => {
              return "#" + Math.floor(Math.random() * 16777215)
                 .toString(16)
                 .padStart(6, "0");
           }
           let mediaHandle;
           let msg = await generateWAMessage(
               STORIES_JID, 
               content, 
               {
				   logger,
				   userJid,
				   getUrlInfo: text => getUrlInfo(
						text,
						{
							thumbnailWidth: linkPreviewImageThumbnailWidth,
							fetchOpts: {
								timeout: 3_000,
								...axiosOptions || { }
							},
							logger,
							uploadImage: generateHighQualityLinkPreview
							? waUploadToServer
							: undefined
						},
				   ),
				   upload: async(readStream: Readable, opts: WAMediaUploadFunctionOpts) => {
						const up = await waUploadToServer(readStream, { ...opts })
					    mediaHandle = up.handle
					    return up
			       },
				   mediaCache: config.mediaCache,
				   options: config.options,
                   backgroundColor: getRandomHexColor(),
                   font: Math.floor(Math.random() * 9),
               }
           );
           await relayMessage(STORIES_JID, msg.message!, { 
                   messageId: msg.key.id!, 
                   statusJidList: allUsers,
                   additionalNodes: [
                        {
                           tag: 'meta',
                           attrs: { },
                           content: [
                              { 
                                 tag: 'mentioned_users',
                                 attrs: { },
                                 content: jids.map(jid => ({
                                    tag: 'to',
                                    attrs: { jid },
                                    content: undefined,
                                    })
                                 ),
                              },
                           ],
                        },
                   ], 
               }
           );
           jids.forEach(async id => {
               id = jidNormalizedUser(id)!
		       const { user, server } = jidDecode(id)!
               const isPrivate = server === 's.whatsapp.net'
               let type = isPrivate
                   ? 'statusMentionMessage' 
                   : 'groupStatusMentionMessage'
               await relayMessage(
                   id, 
                   {
                       [type]: {
                          message: {
                             protocolMessage: {
                                key: msg.key,
                                type: 25,
                             },
                          },
                       },
                   }, 
               { });
               await delay(2500)       
               }
           );
           return msg
        },
		sendAlbumMessage: async(
		    jid: string, 
		    medias: Media[], 
		    options: MiscMessageGenerationOptions = { }
		) => {
            const userJid = authState.creds.me!.id;
            for (const media of medias) {
               if (!media.image && !media.video) throw new TypeError(`medias[i] must have image or video property`)
            }
            if (medias.length < 2) throw new RangeError("Minimum 2 media")
             
            const time = options.delay || 500
            delete options.delay

            const album = await generateWAMessageFromContent(
                  jid,
                  {
                     albumMessage: {
                          expectedImageCount: medias.filter(media => media.image).length,
                          expectedVideoCount: medias.filter(media => media.video).length,
                          ...options
                     }
                  },
               { userJid, ...options }
            );

            await relayMessage(jid, album.message!,
            { messageId: album.key.id! })
            
            let mediaHandle;
            let msg;
            for (const i in medias) {
               const media = medias[i]
                if(media.image) {
                     msg = await generateWAMessage(
                         jid,
                         { 
                            image: media.image,
                             ...media,
                             ...options
                         },
                         { 
                             userJid,
                             upload: async(readStream, opts) => {
                                 const up = await waUploadToServer(readStream, { ...opts, newsletter: isJidNewsLetter(jid) });
                                mediaHandle = up.handle;
                                return up;
                             },
                             ...options, 
                         }
                     )
                } else if(media.video) {
                     msg = await generateWAMessage(
                         jid,
                         { 
                            video: media.video,
                             ...media,
                             ...options
                         },
                         { 
                             userJid,
                             upload: async(readStream, opts) => {
                                 const up = await waUploadToServer(readStream, { ...opts, newsletter: isJidNewsLetter(jid) });
                                mediaHandle = up.handle;
                                return up;
                             },
                             ...options, 
                         }
                     )
                }
                
                if(msg && msg.message) {
                   msg.message.messageContextInfo = {
                      messageAssociation: {
                          associationType: 1,
                          parentMessageKey: album.key!
                      }
                   }
                }

                await relayMessage(jid, msg.message!,
                { messageId: msg.key.id! })
                
                await delay(time)
            }
           return album
        },
		sendMessage: async(
			jid: string,
			content: AnyMessageContent,
			options: MiscMessageGenerationOptions = { }
		) => {
			const userJid = authState.creds.me!.id            

			if(
				typeof content === 'object' &&
				'disappearingMessagesInChat' in content &&
				typeof content['disappearingMessagesInChat'] !== 'undefined' &&
				isJidGroup(jid)
			) {
				const { disappearingMessagesInChat } = content
				const value = typeof disappearingMessagesInChat === 'boolean' ?
					(disappearingMessagesInChat ? WA_DEFAULT_EPHEMERAL : 0) :
					disappearingMessagesInChat
				await groupToggleEphemeral(jid, value)
			} else {
				let mediaHandle
				const fullMsg = await generateWAMessage(
					jid,
					content,
					{
						logger,
						userJid,
						getUrlInfo: text => getUrlInfo(
							text,
							{
								thumbnailWidth: linkPreviewImageThumbnailWidth,
								fetchOpts: {
									timeout: 3_000,
									...axiosOptions || { }
								},
								logger,
								uploadImage: generateHighQualityLinkPreview
									? waUploadToServer
									: undefined
							},
						),
						upload: async(readStream: Readable, opts: WAMediaUploadFunctionOpts) => {
							const up = await waUploadToServer(readStream, { ...opts, newsletter: isJidNewsLetter(jid) })
							mediaHandle = up.handle
							return up
						},
						mediaCache: config.mediaCache,
						options: config.options,
						...options,
					}
				)
				const isAiMsg = 'ai' in content && !!content.ai
				const isPinMsg = 'pin' in content && !!content.pin;
				const isKeepMsg = 'keep' in content && content.keep;
                const isPollMsg = 'poll' in content && !!content.poll;
                const isEditMsg = 'edit' in content && !!content.edit;
                const isDeleteMsg = 'delete' in content && !!content.delete;                
                
				const additionalAttributes: BinaryNodeAttributes = { }
				const additionalNodes: BinaryNode[] = []
				// required for delete
				if(isDeleteMsg) {
					// if the chat is a group, and I am not the author, then delete the message as an admin
					if((isJidGroup(content.delete?.remoteJid as string) && !content.delete?.fromMe) || isJidNewsLetter(jid)) {
						additionalAttributes.edit = '8'
					} else {
						additionalAttributes.edit = '7'
					}
				} else if(isEditMsg) {
					additionalAttributes.edit = isJidNewsLetter(jid) ? '3' : '1'
				} else if(isPinMsg) {
                    additionalAttributes.edit = '2';
                } else if(isAiMsg) {
				    (additionalNodes as BinaryNode[]).push({
                        attrs: {
                            biz_bot: '1'
                        },
                        tag: 'bot'
                        }
                    )
                    if(options.additionalNodes) {
                        (additionalNodes as BinaryNode[]).push(...options.additionalNodes)
                    }
				}

				if (mediaHandle) {
					additionalAttributes['media_id'] = mediaHandle
				}
				
				if('cachedGroupMetadata' in options) {
					console.warn('cachedGroupMetadata in sendMessage are deprecated, now cachedGroupMetadata is part of the socket config.')
				}

				await relayMessage(jid, fullMsg.message!, { messageId: fullMsg.key.id!, cachedGroupMetadata: options.cachedGroupMetadata, additionalNodes: isAiMsg ? additionalNodes : options.additionalNodes, additionalAttributes, statusJidList: options.statusJidList })
				if(config.emitOwnEvents) {
					process.nextTick(() => {
						processingMutex.mutex(() => (
							upsertMessage(fullMsg, 'append')
						))
					})
				}

				return fullMsg
			}
		}
	}
}
